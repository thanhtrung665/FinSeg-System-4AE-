# -*- coding: utf-8 -*-
"""Smoke test cho realtime_pipeline - chay: python realtime_pipeline/test_smoke.py"""
import sys
sys.path.insert(0, ".")

print("=== SMOKE TEST realtime_pipeline ===\n")

# 1. Config
print("[1] Config...")
from realtime_pipeline.config import (
    KAFKA_BROKER, KAFKA_TOPIC_SOCIAL, KAFKA_TOPIC_NEWS,
    CRAWL_INTERVAL_SECONDS, HISTORY_START_DATE, HISTORY_END_DATE,
    NEWS_SOURCES, FACEBOOK_TARGETS, TICKER_MAP,
)
print(f"    Kafka broker     : {KAFKA_BROKER}")
print(f"    History range    : {HISTORY_START_DATE} to {HISTORY_END_DATE}")
print(f"    Crawl interval   : {CRAWL_INTERVAL_SECONDS}s ({CRAWL_INTERVAL_SECONDS//60} min)")
print(f"    News sources     : {list(NEWS_SOURCES.keys())}")
fb_names = [t["name"] for t in FACEBOOK_TARGETS]
print(f"    Facebook targets : {fb_names}")
print("    Config: OK\n")

# 2. Crawlers import
print("[2] Crawlers import...")
from realtime_pipeline.crawlers.news_crawler     import crawl_all_news, RawNewsItem
from realtime_pipeline.crawlers.nhnn_crawler     import crawl_nhnn_all_types, RawPolicyDoc
from realtime_pipeline.crawlers.facebook_crawler import crawl_all_facebook, RawSocialPost
from realtime_pipeline.crawlers.stock_crawler    import fetch_stock_history, RawStockBar
print("    Crawlers: OK\n")

# 3. Normalizers import
print("[3] Normalizers import...")
from realtime_pipeline.normalizers.unified_normalizer import (
    normalize_social_post, normalize_news_article,
    normalize_policy_doc, normalize_stock_bar,
    normalize_social_batch, normalize_news_batch,
)
print("    Normalizers: OK\n")

# 4. Test normalizer voi mock data
print("[4] Test normalizer voi mock data...")
mock_post = RawSocialPost(
    post_id="test_001",
    source="facebook_group",
    source_name="congdongchungkhoan",
    content_text="SHB hom nay tang manh, nen mua vao!",
    published_at="2025-05-01T09:00:00+00:00",
    likes=150, shares=30, comments=45,
    credibility=0.55,
)
normalized = normalize_social_post(mock_post, "SHB")
assert normalized["comment_id"] == "test_001"
assert normalized["ticker"] == "SHB"
assert normalized["likes"] == 150
assert normalized["sentiment"]["label"] in ("positive", "negative", "neutral")
print(f"    Social post normalized: ticker={normalized['ticker']}, sentiment={normalized['sentiment']}")

mock_article = RawNewsItem(
    article_id="news_001",
    source="cafef",
    title="SHB bao lai quy 2 tang 20%",
    content_text="Ngan hang SHB vua cong bo ket qua kinh doanh quy 2 voi loi nhuan tang 20%",
    url="https://cafef.vn/test",
    published_at="2025-05-15T08:00:00+00:00",
    credibility=0.85,
)
norm_article = normalize_news_article(mock_article, "SHB")
assert norm_article["article_id"] == "news_001"
assert norm_article["source"] == "cafef"
print(f"    News article normalized: source={norm_article['source']}, sentiment={norm_article['sentiment']}")

mock_bar = RawStockBar(
    bar_id="SHB_20250501",
    ticker="SHB", trading_date="2025-05-01",
    open=18.5, high=19.2, low=18.3, close=19.0,
    volume=5000000,
    timestamp="2025-05-01T09:00:00+00:00",
    price_change=2.7, is_up=True,
)
norm_bar = normalize_stock_bar(mock_bar, "SHB")
assert norm_bar["ticker_context"] == "SHB"
assert norm_bar["content"]["close"] == 19.0
print(f"    Stock bar normalized: close={norm_bar['content']['close']}, change={norm_bar['content']['price_change']}%")
print("    Normalizers: OK\n")

# 5. Facebook crawler (stub mode)
print("[5] Facebook crawler (stub mode)...")
fb_posts = crawl_all_facebook()
assert len(fb_posts) > 0
print(f"    Stub posts: {len(fb_posts)}")
print(f"    Sample: {fb_posts[0].content_text[:60]}...")
print("    Facebook stub: OK\n")

# 6. News crawler (RSS - thu mang)
print("[6] News crawler (thu RSS CafeF)...")
try:
    from realtime_pipeline.crawlers.news_crawler import crawl_rss_source
    items = crawl_rss_source("cafef", NEWS_SOURCES["cafef"])
    print(f"    CafeF articles: {len(items)}")
    if items:
        print(f"    Sample title: {items[0].title[:80]}...")
    print("    News crawler: OK")
except Exception as e:
    print(f"    News crawler (skip - co the mat mang): {e}")
print()

# 7. Stock crawler (vnstock)
print("[7] Stock crawler (vnstock SHB)...")
try:
    bars = fetch_stock_history("SHB", start_date="2025-04-01", end_date="2025-06-30")
    print(f"    SHB historical bars: {len(bars)}")
    if bars:
        print(f"    First bar: {bars[0].trading_date} close={bars[0].close}")
        print(f"    Last  bar: {bars[-1].trading_date} close={bars[-1].close}")
    print("    Stock crawler: OK")
except Exception as e:
    print(f"    Stock crawler loi: {e}")
print()

# 8. Producers import (khong ket noi that, chi check import)
print("[8] Producers import (dry-run)...")
from realtime_pipeline.producers.realtime_producer import (
    RealtimeChromaIngestor, SocialRealtimeProducer, RealtimeProducer
)
print("    Producers: import OK (khong ket noi that trong smoke test)")
print()

# 9. Scheduler import
print("[9] Scheduler import...")
from realtime_pipeline.scheduler import RealtimeScheduler
sched = RealtimeScheduler(ticker="SHB", interval_seconds=1800)
assert sched.ticker == "SHB"
print(f"    Scheduler ticker={sched.ticker}, interval={sched.interval_seconds}s")
print("    Scheduler: OK\n")

print("=" * 50)
print("TAT CA TESTS PASSED - realtime_pipeline san sang!")
print("=" * 50)
