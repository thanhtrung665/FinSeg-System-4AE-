import ast
import sys
import os
import io
import warnings
import contextlib

warnings.filterwarnings("ignore")
sys.path.insert(0, ".")

print("=" * 55)
print("VERIFY — realtime_pipeline + data_pipeline_ingestion")
print("=" * 55)

# 1. Syntax check
FILES = [
    "dashboard_realtime.py",
    "dashboard.py",
    "realtime_pipeline/config.py",
    "realtime_pipeline/crawlers/stock_crawler.py",
    "realtime_pipeline/crawlers/facebook_crawler.py",
    "realtime_pipeline/crawlers/news_crawler.py",
    "realtime_pipeline/crawlers/nhnn_crawler.py",
    "realtime_pipeline/normalizers/unified_normalizer.py",
    "realtime_pipeline/producers/realtime_producer.py",
    "realtime_pipeline/vmsi_realtime.py",
    "realtime_pipeline/scheduler.py",
    "data_pipeline_ingestion/vnstock_producer.py",
    "data_pipeline_ingestion/market_producer.py",
    "data_pipeline_ingestion/config.py",
    "data_pipeline_ingestion/normalizer.py",
    "data_pipeline_ingestion/base_producer.py",
]
errors = []
for f in FILES:
    try:
        with open(f, "rb") as fp:
            src = fp.read().decode("utf-8")
        ast.parse(src)
        print(f"  [OK] {f}")
    except SyntaxError as e:
        errors.append(f)
        print(f"  [ERR] {f}: {e}")
    except FileNotFoundError:
        print(f"  [?]  {f}: not found")

print()

# 2. vnstock moi — tat banner bang redirect
print("[2] vnstock API (moi, khong banner)...")
buf = io.StringIO()
with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
    from vnstock.api.quote import Quote
    q  = Quote(symbol="SHB", source="VCI")
    df = q.history(start="2025-05-01", end="2025-05-31", interval="1D")
output = buf.getvalue()
print(f"    SHB May-2025: {len(df)} bars, close_last={df.iloc[-1]['close']}")
if output.strip():
    print(f"    [!] vnstock viet gi ra stdout/stderr: {repr(output[:80])}")
else:
    print("    stdout/stderr sach (khong co banner)")

# 3. Facebook stub — khong warning
print("[3] Facebook crawler (stub, khong WARNING)...")
import logging
# Bat log de kiem tra khong co WARNING
handler = logging.handlers = []
test_log = io.StringIO()
test_handler = logging.StreamHandler(test_log)
test_handler.setLevel(logging.WARNING)
fb_logger = logging.getLogger("realtime_pipeline.crawlers.facebook_crawler")
fb_logger.addHandler(test_handler)
fb_logger.setLevel(logging.DEBUG)

from realtime_pipeline.crawlers.facebook_crawler import crawl_all_facebook
posts = crawl_all_facebook()
fb_logger.removeHandler(test_handler)

warn_output = test_log.getvalue()
if warn_output.strip():
    print(f"    [!] Co WARNING: {repr(warn_output[:120])}")
else:
    print(f"    Khong co WARNING. {len(posts)} stub posts.")

# 4. Config realtime
print("[4] Realtime config...")
from realtime_pipeline.config import (
    KAFKA_BROKER, HISTORY_START_DATE, HISTORY_END_DATE,
    NEWS_SOURCES, FACEBOOK_TARGETS,
)
print(f"    Kafka={KAFKA_BROKER}, History={HISTORY_START_DATE}..{HISTORY_END_DATE}")
print(f"    News={list(NEWS_SOURCES.keys())}, FB={[t['name'] for t in FACEBOOK_TARGETS]}")

# 5. Normalizer
print("[5] Unified normalizer...")
from realtime_pipeline.crawlers.facebook_crawler import RawSocialPost
from realtime_pipeline.normalizers.unified_normalizer import normalize_social_post
p = RawSocialPost("id1","group","test","SHB tang manh hom nay","2025-05-01T00:00:00Z",100,10,20,0.55)
n = normalize_social_post(p, "SHB")
assert n["ticker"] == "SHB" and n["likes"] == 100
print(f"    Normalize OK: ticker={n['ticker']}, sentiment={n['sentiment']}")

print()
print("=" * 55)
if errors:
    print(f"FAILED: {len(errors)} file co loi syntax: {errors}")
    sys.exit(1)
else:
    print("TAT CA KIEM TRA PASSED")
print("=" * 55)
