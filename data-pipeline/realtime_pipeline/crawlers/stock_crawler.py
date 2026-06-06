import contextlib
import io
import dataclasses # THÊM IMPORT NÀY Ở ĐẦU FILE
import logging
import warnings
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, List, Optional

from realtime_pipeline.config import (
    HISTORY_START_DATE, HISTORY_END_DATE, TICKER_MAP
)

logger = logging.getLogger(__name__)

# Tat het warning / stdout cua vnstock ngay khi module duoc load
warnings.filterwarnings("ignore")


# ── Suppress helper ────────────────────────────────────────────────────────────
class _Quiet(contextlib.AbstractContextManager):
    """Redirect ca stdout va stderr de tat banner quang cao vnstock."""
    def __enter__(self):
        self._buf = io.StringIO()
        self._so  = contextlib.redirect_stdout(self._buf).__enter__()
        self._se  = contextlib.redirect_stderr(self._buf).__enter__()
        return self

    def __exit__(self, *args):
        self._so.__exit__(*args)
        self._se.__exit__(*args)
        return False  # khong suppress exceptions


# Import vnstock API moi mot lan, tat tat ca output
with _Quiet():
    try:
        from vnstock.api.quote import Quote as _VnQuote
        _VNSTOCK_AVAILABLE = True
    except Exception:
        _VnQuote = None
        _VNSTOCK_AVAILABLE = False


def _get_quote(symbol: str):
    """Tao Quote instance, suppress moi output."""
    with _Quiet():
        return _VnQuote(symbol=symbol, source="VCI")


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
    Lay lich su OHLCV tu start_date → end_date.
    Mac dinh: HISTORY_START_DATE → HISTORY_END_DATE (thang 4-6 hien tai).
    """
    if not _VNSTOCK_AVAILABLE:
        logger.error("vnstock khong kha dung")
        return []

    start  = start_date or HISTORY_START_DATE
    end    = end_date   or HISTORY_END_DATE
    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    bars: List[RawStockBar] = []

    try:
        logger.info(f"[Stock] Lay lich su {symbol} ({start} to {end})...")
        q  = _get_quote(symbol)
        with _Quiet():
            df = q.history(start=start, end=end, interval="1D")

        if df is None or df.empty:
            logger.warning(f"Khong co du lieu lich su cho {symbol}")
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
        logger.error(f"Loi lay lich su {symbol}: {e}")
        return []


def fetch_stock_realtime(ticker: str) -> Optional[RawStockBar]:
    """Lay gia co phieu phien hien tai hoac phien cuoi cung."""
    if not _VNSTOCK_AVAILABLE:
        return None

    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    try:
        q     = _get_quote(symbol)
        today = date.today().strftime("%Y-%m-%d")

        with _Quiet():
            df = q.history(start=today, end=today, interval="1D")

        if df is None or df.empty:
            yesterday = (date.today() - timedelta(days=3)).strftime("%Y-%m-%d")
            with _Quiet():
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
        logger.error(f"Loi lay realtime {symbol}: {e}")
        return None


def fetch_stock_info(ticker: str) -> RawStockInfo:
    """Lay thong tin co ban (fallback gracefully neu API khong ho tro)."""
    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    if not _VNSTOCK_AVAILABLE:
        return RawStockInfo(ticker=symbol, timestamp=datetime.now(timezone.utc).isoformat())

    try:
        q = _get_quote(symbol)
        if hasattr(q, "profile"):
            with _Quiet():
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
        logger.debug(f"Khong lay duoc thong tin co ban {symbol}: {e}")

    return RawStockInfo(ticker=symbol, timestamp=datetime.now(timezone.utc).isoformat())


# ── Sentiment calculator ───────────────────────────────────────────────────────

def calculate_market_sentiment(bars: List[RawStockBar]) -> float:
    """
    Tinh diem cam xuc gia thi truong trong [-1.0, 1.0].
    Logic: price_change >= +2% → +1.0  |  > 0 → +0.5
           price_change <= -2% → -1.0  |  < 0 → -0.5
    """
    if not bars:
        return 0.0

    scores = []
    for bar in bars:
        chg = bar.price_change
        if chg >= 2.0:
            scores.append(1.0)
        elif chg > 0:
            scores.append(0.5)
        elif chg <= -2.0:
            scores.append(-1.0)
        elif chg < 0:
            scores.append(-0.5)
        else:
            scores.append(0.0)

    return round(sum(scores) / len(scores), 4)


# ── Public API ─────────────────────────────────────────────────────────────────

def crawl_stocks_for_ticker(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Thu thap day du du lieu co phieu cho 1 ticker.
    Tra ve dict chuan JSON: historical_bars, realtime_bar, info, market_sentiment.
    """
    symbol = TICKER_MAP.get(ticker.upper(), ticker.upper())
    logger.info(f"[Stock] Crawl {symbol} (context={ticker})...")

    bars          = fetch_stock_history(ticker, start_date=start_date, end_date=end_date)
    rt            = fetch_stock_realtime(ticker)
    info          = fetch_stock_info(ticker)
    mkt_sentiment = calculate_market_sentiment(
        bars[-10:] if len(bars) > 10 else bars
    )

    # DÙNG dataclasses.asdict() ĐỂ CHUYỂN ĐỔI SANG JSON FORMAT
    return {
        "ticker":           symbol,
        "ticker_context":   ticker,
        "historical_bars":  [dataclasses.asdict(b) for b in bars], # Chuyển list object thành list dict
        "realtime_bar":     dataclasses.asdict(rt) if rt else None,
        "info":             dataclasses.asdict(info) if info else None,
        "market_sentiment": mkt_sentiment,
        "total_bars":       len(bars),
        "source_type":      "market"
    }