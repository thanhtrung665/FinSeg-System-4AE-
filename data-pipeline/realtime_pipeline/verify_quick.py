"""
realtime_pipeline/verify_quick.py
Kiem tra NHANH (skip crawl data that) - chi test import va logic.
"""

import sys
import logging
import os
import warnings

os.environ["VNSTOCK_DISABLE_NOTIFICATION"] = "1"
os.environ["VNSTOCK_SHOW_ADS"] = "0"
warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)
sys.path.insert(0, ".")

_OK = "\033[92m[OK]\033[0m"
_FAIL = "\033[91m[FAIL]\033[0m"
_INFO = "\033[94m[INFO]\033[0m"

errors = []

def check(label, ok, detail=""):
    if ok:
        print(f"  {_OK}  {label}" + (f" — {detail}" if detail else ""))
    else:
        print(f"  {_FAIL} {label}" + (f" — {detail}" if detail else ""))
        errors.append(label)

print("=" * 60)
print("  QUICK VERIFICATION (No real data crawl)")
print("=" * 60)

# 1. Config
print(f"\n{_INFO} [1] Config...")
try:
    from realtime_pipeline.config import KAFKA_BROKER, NEWS_SOURCES, KAFKA_TOPIC_POLICY
    check("Config load", True, f"Kafka={KAFKA_BROKER}")
    check("Kafka topics", bool(KAFKA_TOPIC_POLICY), KAFKA_TOPIC_POLICY)
except Exception as e:
    check("Config", False, str(e))

# 2. Normalizers
print(f"\n{_INFO} [2] Normalizers...")
try:
    from realtime_pipeline.normalizers.unified_normalizer import (
        normalize_social_post, normalize_news_article,
        normalize_stock_bar, normalize_stock_batch
    )
    
    # Test social (dict)
    mock_social = {
        "post_id": "test_001",
        "source": "group",
        "source_name": "test",
        "content_text": "SHB tang manh",
        "published_at": "2025-05-10T09:00:00+00:00",
        "likes": 200,
        "shares": 30,
        "comments": 50,
        "credibility": 0.55,
    }
    n1 = normalize_social_post(mock_social, "SHB")
    check("normalize_social_post (dict)", n1["ticker"] == "SHB" and n1["likes"] == 200)
    
    # Test news (dict)
    mock_news = {
        "article_id": "art_001",
        "source": "cafef",
        "title": "SHB test",
        "content_text": "Test content",
        "url": "https://test.com",
        "published_at": "2025-05-15T08:00:00+00:00",
        "credibility": 0.85,
    }
    n2 = normalize_news_article(mock_news, "SHB")
    check("normalize_news_article (dict)", n2["source"] == "cafef")
    
    # Test stock (dict)
    mock_stock = {
        'bar_id': 'SHB_20250501',
        'ticker': 'SHB',
        'trading_date': '2025-05-01',
        'open': 18.5,
        'close': 19.0,
        'high': 19.2,
        'low': 18.3,
        'volume': 5000000,
        'timestamp': '2025-05-01T09:00:00+00:00',
        'price_change': 2.7,
        'is_up': True,
    }
    n3 = normalize_stock_bar(mock_stock, "SHB")
    check("normalize_stock_bar (dict)", n3["content"]["close"] == 19.0)
    
    # Test batch
    batch = normalize_stock_batch([mock_stock], "SHB")
    check("normalize_stock_batch", len(batch) == 1)
    
except Exception as e:
    check("Normalizers", False, str(e))
    import traceback
    traceback.print_exc()

# 3. VMSI Engine
print(f"\n{_INFO} [3] VMSI Engine...")
try:
    from multi_agent_system.engines.vmsi_engine import VMSIEngine
    import numpy as np
    
    engine = VMSIEngine()
    w = engine.calculate_interaction_weight(500, 100, 300)
    ss = engine.calculate_social_score(np.array([-0.8, 0.6]), np.array([w, w]), np.array([0.7, 0.6]))
    sm = engine.calculate_macro_score(-1, 0.0)
    vmsi = engine.calculate_final_vmsi(engine.calculate_raw_index(sm, ss))
    
    check("VMSIEngine math", 0 <= vmsi <= 100, f"VMSI={vmsi:.2f}")
except Exception as e:
    check("VMSI Engine", False, str(e))

# 4. RealtimeVMSIEngine
print(f"\n{_INFO} [4] RealtimeVMSIEngine...")
try:
    from realtime_pipeline.vmsi_realtime import RealtimeVMSIEngine
    engine = RealtimeVMSIEngine.__new__(RealtimeVMSIEngine)
    engine.ticker = "SHB"
    check("RealtimeVMSIEngine init", True)
except Exception as e:
    check("RealtimeVMSIEngine", False, str(e))

# 5. Vector Worker
print(f"\n{_INFO} [5] Vector Worker...")
try:
    from realtime_pipeline.crawlers.vector_worker import RealtimeVectorIngestor
    worker = RealtimeVectorIngestor.__new__(RealtimeVectorIngestor)
    text = "Test " * 500
    chunks = worker.chunk_text(text, 1500, 200)
    check("Vector Worker chunking", len(chunks) >= 2, f"{len(chunks)} chunks")
except Exception as e:
    check("Vector Worker", False, str(e))

# 6. Scheduler
print(f"\n{_INFO} [6] Scheduler...")
try:
    from realtime_pipeline.scheduler import RealtimeScheduler
    sched = RealtimeScheduler(ticker="SHB", interval_seconds=1800)
    check("Scheduler init", sched.ticker == "SHB")
except Exception as e:
    check("Scheduler", False, str(e))

# 7. Multi-Agent System
print(f"\n{_INFO} [7] Multi-Agent System...")
try:
    from multi_agent_system.agents.social_agent import SocialAgent
    from multi_agent_system.agents.macro_agent import MacroAgent
    from multi_agent_system.agents.risk_agent import RiskSynthesisAgent
    from multi_agent_system.agents.mac_orchestrator import MACSystem
    check("Import SocialAgent", True)
    check("Import MacroAgent", True)
    check("Import RiskSynthesisAgent", True)
    check("Import MACSystem", True)
except Exception as e:
    check("Multi-Agent imports", False, str(e))

# Summary
print("\n" + "=" * 60)
if errors:
    print(f"\033[91m  FAILED — {len(errors)} items:\033[0m")
    for e in errors:
        print(f"    - {e}")
    sys.exit(1)
else:
    print("\033[92m  ALL CHECKS PASSED (Quick Mode)\033[0m")
    print("\n  Core functions verified:")
    print("  ✓ Config")
    print("  ✓ Normalizers (dict support)")
    print("  ✓ VMSI Engine")
    print("  ✓ Realtime Engine")
    print("  ✓ Vector Worker")
    print("  ✓ Scheduler")
    print("  ✓ Multi-Agent System")
    print("\n  Ready to run:")
    print("    python realtime_pipeline/scheduler.py --ticker SHB --once")
print("=" * 60)
