# Fix: Stock Normalizer Missing Functions

## 🐛 Problem

File `unified_normalizer.py` đã xóa các functions:
- `normalize_stock_bar()`
- `normalize_stock_batch()`

Nhưng các file sau vẫn đang import và sử dụng chúng:
- `realtime_pipeline/vmsi_realtime.py` ❌
- `realtime_pipeline/verify.py` ❌
- `realtime_pipeline/test_smoke.py` ❌

**Lỗi runtime:**
```python
ImportError: cannot import name 'normalize_stock_bar' from 'realtime_pipeline.normalizers.unified_normalizer'
```

---

## ✅ Solution

### 1. Thêm lại functions vào `unified_normalizer.py`

**File:** `realtime_pipeline/normalizers/unified_normalizer.py`

```python
def normalize_stock_bar(bar: Any, ticker_context: str = "") -> Dict[str, Any]:
    """
    Chuẩn hóa 1 RawStockBar thành schema Kafka.
    bar có thể là dataclass RawStockBar hoặc dict.
    """
    # Nếu bar là dataclass, convert sang dict
    if hasattr(bar, '__dataclass_fields__'):
        bar_dict = {
            'bar_id': bar.bar_id,
            'ticker': bar.ticker,
            'trading_date': bar.trading_date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'timestamp': bar.timestamp,
            'ticker_context': bar.ticker_context,
            'source': getattr(bar, 'source', 'vnstock'),
            'source_type': getattr(bar, 'source_type', 'market'),
            'price_change': getattr(bar, 'price_change', 0.0),
            'is_up': getattr(bar, 'is_up', False),
        }
    else:
        bar_dict = bar
    
    ctx = ticker_context or bar_dict.get('ticker_context', bar_dict.get('ticker', 'UNKNOWN'))
    
    return {
        "bar_id":            bar_dict.get('bar_id'),
        "ticker_context":    ctx,
        "ticker":            bar_dict.get('ticker'),
        "trading_date":      bar_dict.get('trading_date'),
        "timestamp":         bar_dict.get('timestamp'),
        "timestamp_ingested": _now_iso(),
        "source":            bar_dict.get('source', 'vnstock'),
        "source_type":       bar_dict.get('source_type', 'market'),
        "content": {
            "open":          float(bar_dict.get('open', 0)),
            "high":          float(bar_dict.get('high', 0)),
            "low":           float(bar_dict.get('low', 0)),
            "close":         float(bar_dict.get('close', 0)),
            "volume":        int(bar_dict.get('volume', 0)),
            "price_change":  float(bar_dict.get('price_change', 0.0)),
            "is_up":         bool(bar_dict.get('is_up', False)),
        },
        "normalized": True,
    }


def normalize_stock_batch(bars: List[Any], ticker_context: str = "") -> List[Dict[str, Any]]:
    """Chuẩn hóa batch các RawStockBar."""
    return [normalize_stock_bar(b, ticker_context) for b in bars]
```

### 2. Fix import trong `vmsi_realtime.py`

**File:** `realtime_pipeline/vmsi_realtime.py`

**Trước:**
```python
from realtime_pipeline.normalizers.unified_normalizer import normalize_stock_batch

# ...later in code
from realtime_pipeline.normalizers.unified_normalizer import normalize_stock_bar
```

**Sau:**
```python
from realtime_pipeline.normalizers.unified_normalizer import (
    normalize_stock_batch, normalize_stock_bar
)
```

---

## 🧪 Testing

### Test 1: Quick Test
```bash
cd data-pipeline
python realtime_pipeline/test_stock_normalizer.py
```

**Expected Output:**
```
============================================================
Test Stock Normalizer Functions
============================================================

[1] Testing imports...
✓ Successfully imported normalize_stock_bar and normalize_stock_batch

[2] Testing with dataclass object...
✓ Dataclass normalization OK

[3] Testing with dict object...
✓ Dict normalization OK

[4] Testing batch normalization...
✓ Batch normalization OK

[5] Testing import in vmsi_realtime...
✓ RealtimeVMSIEngine import OK

============================================================
✅ ALL TESTS PASSED
============================================================
```

### Test 2: Full System Verify
```bash
python realtime_pipeline/verify.py
```

**Expected:** All 12 checks should pass.

### Test 3: Run One Cycle
```bash
python realtime_pipeline/scheduler.py --ticker SHB --once
```

**Expected:** VMSI calculated successfully without import errors.

---

## 📊 Why This Fix Works

### Problem Analysis

1. **`stock_crawler.py` returns `RawStockBar` dataclass objects**
   - Not plain dicts
   - Has specific fields: `bar_id`, `ticker`, `open`, `close`, etc.

2. **Kafka expects standardized JSON schema**
   - Need to convert dataclass → dict
   - Add metadata: `timestamp_ingested`, `normalized`, etc.
   - Nest price data under `content` key

3. **Previous code assumed normalizer existed**
   - `vmsi_realtime.py` called `normalize_stock_batch(bars)`
   - Function was removed from `unified_normalizer.py`
   - Runtime error when scheduler runs

### Solution Approach

✅ **Add back normalizer functions** with dual support:
- Handle `RawStockBar` dataclass (from `stock_crawler.py`)
- Handle dict objects (for flexibility)

✅ **Use `hasattr(bar, '__dataclass_fields__')` to detect dataclass**
- If dataclass → convert to dict manually
- If dict → use directly

✅ **Generate proper schema for Kafka**
- All required fields
- Nested `content` structure
- Metadata fields

---

## 🔍 Technical Details

### RawStockBar Structure (Dataclass)
```python
@dataclass
class RawStockBar:
    bar_id: str
    ticker: str
    trading_date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp: str
    ticker_context: str = ""
    source: str = "vnstock"
    source_type: str = "market"
    price_change: float = 0.0
    is_up: bool = False
```

### Normalized Schema (Output)
```python
{
    "bar_id": "SHB_20250501",
    "ticker_context": "SHB",
    "ticker": "SHB",
    "trading_date": "2025-05-01",
    "timestamp": "2025-05-01T09:00:00+00:00",
    "timestamp_ingested": "2026-06-06T10:30:00.123456+00:00",
    "source": "vnstock",
    "source_type": "market",
    "content": {
        "open": 18.5,
        "high": 19.2,
        "low": 18.3,
        "close": 19.0,
        "volume": 5000000,
        "price_change": 2.7,
        "is_up": true
    },
    "normalized": true
}
```

---

## 📝 Files Changed

| File | Change | Status |
|------|--------|--------|
| `realtime_pipeline/normalizers/unified_normalizer.py` | Added `normalize_stock_bar()` and `normalize_stock_batch()` | ✅ Fixed |
| `realtime_pipeline/vmsi_realtime.py` | Fixed import statement | ✅ Fixed |
| `realtime_pipeline/test_stock_normalizer.py` | New test file | ✅ Added |
| `FIX_STOCK_NORMALIZER.md` | This document | ✅ Added |

---

## ✅ Verification Checklist

- [x] Functions added to `unified_normalizer.py`
- [x] Import fixed in `vmsi_realtime.py`
- [x] Test script created and passed
- [x] Dataclass support verified
- [x] Dict support verified
- [x] Batch processing verified
- [x] Integration with `RealtimeVMSIEngine` verified

---

## 🚀 Next Steps

After this fix, you can:

1. ✅ Run full system verify:
   ```bash
   python realtime_pipeline/verify.py
   ```

2. ✅ Test one VMSI cycle:
   ```bash
   python realtime_pipeline/scheduler.py --ticker SHB --once
   ```

3. ✅ Start production system:
   ```bash
   # Linux/Mac
   bash realtime_pipeline/manage_processes.sh start
   
   # Windows
   realtime_pipeline\manage_processes.bat start
   ```

---

## 📞 Support

If you encounter any issues:

1. Check test output:
   ```bash
   python realtime_pipeline/test_stock_normalizer.py
   ```

2. Check imports:
   ```python
   from realtime_pipeline.normalizers.unified_normalizer import normalize_stock_bar
   print("✓ Import OK")
   ```

3. Check logs:
   ```bash
   tail -f logs/scheduler.log
   ```

---

**Fix Date:** 2026-06-06  
**Status:** ✅ Resolved  
**Tested:** ✅ All tests passed
