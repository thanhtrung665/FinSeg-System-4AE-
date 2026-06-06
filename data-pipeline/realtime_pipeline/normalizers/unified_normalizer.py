# -*- coding: utf-8 -*-
"""
realtime_pipeline/normalizers/unified_normalizer.py

Chuan hoa tat ca nguon du lieu thanh Standard JSON thong nhat,
tuong thich voi schema ma SocialAgent / MacroAgent / RiskAgent can.

Schema dau ra:
  - Social message  → fb_mock_data schema (tuong thich SocialAgent)
  - News article    → realtime_news schema (Kafka + ChromaDB)
  - Policy doc      → realtime_policy schema (ChromaDB ingest)
  - Stock bar       → market_stock_data schema (tuong thich vnstock producer)
"""

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from realtime_pipeline.crawlers.news_crawler     import RawNewsItem
from realtime_pipeline.crawlers.nhnn_crawler     import RawPolicyDoc
from realtime_pipeline.crawlers.facebook_crawler import RawSocialPost
from realtime_pipeline.crawlers.stock_crawler    import RawStockBar


# ── Sentiment heuristic dung chung ────────────────────────────────────────────
_NEGATIVE_KW = [
    "giam", "ban thao", "rut tien", "sup", "vo no", "lua dao",
    "tieu cuc", "canh bao", "nguy hiem", "rui ro", "siet chat",
    "xu ly nghiem", "khoi to", "bat giam", "thanh tra", "vi pham",
    "cuu", "sap", "bat day", "hoang loan", "tut doc",
]
_POSITIVE_KW = [
    "tang", "mua vao", "bung pha", "tot", "tich cuc", "khuyen nghi",
    "ho tro", "noi long", "giam lai suat", "bom tien", "khuyen khich",
    "tang truong", "ky vong", "dot pha", "xuat sac", "muc", "tim",
    "len", "tang manh", "vot len",
]

def _keyword_sentiment(text: str) -> float:
    """
    Tinh diem sentiment tu keyword [-1.0, 1.0].
    Dung lam fallback khi chua co PhoBERT.
    """
    text_lower = text.lower()
    neg = sum(1 for kw in _NEGATIVE_KW if kw in text_lower)
    pos = sum(1 for kw in _POSITIVE_KW if kw in text_lower)
    total = neg + pos
    if total == 0:
        return 0.0
    score = (pos - neg) / total
    return round(max(-1.0, min(1.0, score)), 4)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Normalizers ────────────────────────────────────────────────────────────────

def normalize_social_post(post: RawSocialPost, ticker_context: str = "") -> Dict[str, Any]:
    """
    Chuan hoa RawSocialPost → schema tuong thich voi fb_mock_data
    (SocialAgent.process_message_batch() doc duoc).

    Output khop voi FacebookMockData schema cua normalizer.py cu.
    """
    ctx = ticker_context or post.ticker_context or "UNKNOWN"
    sentiment_score = _keyword_sentiment(post.content_text)

    return {
        # Core fields khop voi FacebookMockData
        "comment_id":        post.post_id,
        "ticker":            ctx,
        "content_text":      post.content_text,
        "created_at":        post.published_at,
        "likes":             max(0, int(post.likes)),
        "shares":            max(0, int(post.shares)),
        "comments":          max(0, int(post.comments)),
        "timestamp_ingested": _now_iso(),
        "normalized":        True,
        "source_dataset":    f"facebook_{post.source_name}",
        # Extra fields cho realtime pipeline
        "ticker_context":    ctx,
        "source":            post.source,
        "source_name":       post.source_name,
        "url":               post.url,
        "credibility":       post.credibility,
        "sentiment": {
            "label":      "positive" if sentiment_score > 0.1
                          else ("negative" if sentiment_score < -0.1 else "neutral"),
            "confidence": abs(sentiment_score),
        },
    }


def normalize_news_article(article: RawNewsItem, ticker_context: str = "") -> Dict[str, Any]:
    """
    Chuan hoa RawNewsItem → schema Kafka realtime_news.
    Cung duoc dung de ingest vao ChromaDB (tuong tu nhnn_ingestor).
    """
    ctx = ticker_context or article.ticker_context or "UNKNOWN"
    sentiment_score = _keyword_sentiment(article.content_text)

    return {
        "article_id":        article.article_id,
        "ticker_context":    ctx,
        "source":            article.source,
        "title":             article.title,
        "content_text":      article.content_text,
        "url":               article.url,
        "published_at":      article.published_at,
        "timestamp_ingested": _now_iso(),
        "credibility":       article.credibility,
        "source_type":       "news",
        "sentiment": {
            "label":      "positive" if sentiment_score > 0.1
                          else ("negative" if sentiment_score < -0.1 else "neutral"),
            "confidence": abs(sentiment_score),
        },
        # Dung cho SocialAgent (neu producer day vao fb_mock_data topic)
        "comment_id":    article.article_id,
        "ticker":        ctx,
        "likes":         0,
        "shares":        0,
        "comments":      0,
        "normalized":    True,
        "source_dataset": f"news_{article.source}",
    }


def normalize_policy_doc(doc: RawPolicyDoc, ticker_context: str = "") -> Dict[str, Any]:
    """
    Chuan hoa RawPolicyDoc → metadata dict cho ChromaDB ingest.
    Format giong nhnn_ingestor.py.
    """
    ctx = ticker_context or doc.ticker_context or "POLICY"

    return {
        "doc_id":          doc.doc_id,
        "ticker_context":  ctx,
        "source":          "nhnn",
        "doc_type":        doc.doc_type,
        "title":           doc.title,
        "content_text":    doc.content_text,
        "url":             doc.url,
        "published_at":    doc.published_at,
        "timestamp_ingested": _now_iso(),
        "credibility":     1.0,
        "source_type":     "policy",
        # ChromaDB metadata fields (tuong tu nhnn_ingestor)
        "metadata": {
            "source_file":    doc.doc_id,
            "ticker_context": ctx,
            "publish_date":   doc.published_at,
            "document_type":  "policy_regulation",
            "chunk_index":    0,
            "doc_type":       doc.doc_type,
            "url":            doc.url,
        },
    }


def normalize_stock_bar(bar: RawStockBar, ticker_context: str = "") -> Dict[str, Any]:
    """
    Chuan hoa RawStockBar → schema tuong thich voi market_stock_data Kafka topic.
    """
    ctx = ticker_context or bar.ticker_context or bar.ticker

    return {
        "timestamp":      bar.timestamp,
        "ticker_context": ctx,
        "source":         "vnstock",
        "source_type":    "market",
        "content": {
            "trading_date": bar.trading_date,
            "open":         bar.open,
            "high":         bar.high,
            "low":          bar.low,
            "close":        bar.close,
            "volume":       bar.volume,
            "price_change": bar.price_change,
            "is_up":        bar.is_up,
        },
    }


# ── Batch normalizers ──────────────────────────────────────────────────────────

def normalize_social_batch(
    posts: List[RawSocialPost], ticker_context: str = ""
) -> List[Dict[str, Any]]:
    return [normalize_social_post(p, ticker_context) for p in posts]


def normalize_news_batch(
    articles: List[RawNewsItem], ticker_context: str = ""
) -> List[Dict[str, Any]]:
    return [normalize_news_article(a, ticker_context) for a in articles]


def normalize_policy_batch(
    docs: List[RawPolicyDoc], ticker_context: str = ""
) -> List[Dict[str, Any]]:
    return [normalize_policy_doc(d, ticker_context) for d in docs]


def normalize_stock_batch(
    bars: List[RawStockBar], ticker_context: str = ""
) -> List[Dict[str, Any]]:
    return [normalize_stock_bar(b, ticker_context) for b in bars]
