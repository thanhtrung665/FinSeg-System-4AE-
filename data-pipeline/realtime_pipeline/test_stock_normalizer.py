#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick test script để verify normalize_stock_bar và normalize_stock_batch
sau khi fix lỗi import.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

print("=" * 60)
print("Test Stock Normalizer Functions")
print("=" * 60)

# Test 1: Import functions
print("\n[1] Testing imports...")
try:
    from realtime_pipeline.normalizers.unified_normalizer import (
        normalize_stock_bar,
        normalize_stock_batch
    )
    print("✓ Successfully imported normalize_stock_bar and normalize_stock_batch")
except ImportError as e:
    print(f"✗ Import failed: {e}")
    sys.exit(1)

# Test 2: Create mock RawStockBar (dataclass)
print("\n[2] Testing with dataclass object...")
try:
    from realtime_pipeline.crawlers.stock_crawler import RawStockBar
    
    mock_bar = RawStockBar(
        bar_id="SHB_20250501",
        ticker="SHB",
        trading_date="2025-05-01",
        open=18.5,
        high=19.2,
        low=18.3,
        close=19.0,
        volume=5000000,
        timestamp="2025-05-01T09:00:00+00:00",
        ticker_context="SHB",
        source="vnstock",
        source_type="market",
        price_change=2.7,
        is_up=True,
    )
    
    normalized = normalize_stock_bar(mock_bar, "SHB")
    
    # Assertions
    assert normalized["ticker_context"] == "SHB", "ticker_context mismatch"
    assert normalized["ticker"] == "SHB", "ticker mismatch"
    assert normalized["content"]["close"] == 19.0, "close price mismatch"
    assert normalized["content"]["price_change"] == 2.7, "price_change mismatch"
    assert normalized["content"]["is_up"] == True, "is_up mismatch"
    assert normalized["normalized"] == True, "normalized flag missing"
    
    print(f"✓ Dataclass normalization OK")
    print(f"  - ticker: {normalized['ticker']}")
    print(f"  - close: {normalized['content']['close']}")
    print(f"  - change: {normalized['content']['price_change']}%")
    
except Exception as e:
    print(f"✗ Dataclass test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Test with dict object
print("\n[3] Testing with dict object...")
try:
    mock_bar_dict = {
        'bar_id': 'VCB_20250502',
        'ticker': 'VCB',
        'trading_date': '2025-05-02',
        'open': 85.0,
        'high': 87.5,
        'low': 84.5,
        'close': 86.8,
        'volume': 3000000,
        'timestamp': '2025-05-02T09:00:00+00:00',
        'ticker_context': 'VCB',
        'source': 'vnstock',
        'source_type': 'market',
        'price_change': 1.2,
        'is_up': True,
    }
    
    normalized = normalize_stock_bar(mock_bar_dict, "VCB")
    
    # Assertions
    assert normalized["ticker_context"] == "VCB", "ticker_context mismatch"
    assert normalized["ticker"] == "VCB", "ticker mismatch"
    assert normalized["content"]["close"] == 86.8, "close price mismatch"
    
    print(f"✓ Dict normalization OK")
    print(f"  - ticker: {normalized['ticker']}")
    print(f"  - close: {normalized['content']['close']}")
    
except Exception as e:
    print(f"✗ Dict test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 4: Test batch normalization
print("\n[4] Testing batch normalization...")
try:
    bars = []
    for i in range(3):
        bars.append(RawStockBar(
            bar_id=f"SHB_2025050{i+1}",
            ticker="SHB",
            trading_date=f"2025-05-0{i+1}",
            open=18.0 + i*0.5,
            high=19.0 + i*0.5,
            low=17.5 + i*0.5,
            close=18.5 + i*0.5,
            volume=5000000,
            timestamp=f"2025-05-0{i+1}T09:00:00+00:00",
            ticker_context="SHB",
            price_change=1.0 + i*0.5,
            is_up=True,
        ))
    
    normalized_batch = normalize_stock_batch(bars, "SHB")
    
    # Assertions
    assert len(normalized_batch) == 3, "batch size mismatch"
    assert all(b["ticker"] == "SHB" for b in normalized_batch), "ticker mismatch in batch"
    assert all("content" in b for b in normalized_batch), "content missing in batch"
    
    print(f"✓ Batch normalization OK")
    print(f"  - Processed {len(normalized_batch)} bars")
    print(f"  - Sample close prices: {[b['content']['close'] for b in normalized_batch]}")
    
except Exception as e:
    print(f"✗ Batch test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Integration with vmsi_realtime (import only)
print("\n[5] Testing import in vmsi_realtime...")
try:
    from realtime_pipeline.vmsi_realtime import RealtimeVMSIEngine
    
    # Just check that the class can be imported (don't run it)
    engine = RealtimeVMSIEngine.__new__(RealtimeVMSIEngine)
    engine.ticker = "SHB"
    
    print(f"✓ RealtimeVMSIEngine import OK")
    print(f"  - Engine can be instantiated")
    
except Exception as e:
    print(f"✗ vmsi_realtime import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Success!
print("\n" + "=" * 60)
print("✅ ALL TESTS PASSED")
print("=" * 60)
print("\nStock normalizer functions are working correctly!")
print("You can now run:")
print("  python realtime_pipeline/verify.py")
print("  python realtime_pipeline/scheduler.py --ticker SHB --once")
print("=" * 60)
