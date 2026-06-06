# -*- coding: utf-8 -*-
"""
realtime_pipeline/normalizers/unified_normalizer.py

Chuẩn hóa tất cả nguồn dữ liệu thành Standard JSON thống nhất,
tương thích với schema mà SocialAgent / MacroAgent / RiskAgent cần.
Cập nhật: Nhận đầu vào là Dictionary thay vì DataClass.
"""

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

# ── Sentiment heuristic dùng chung ────────────────────────────────────────────
_NEGATIVE_KW = [
    "giảm", "bán tháo", "rút tiền", "sập", "vỡ nợ", "lừa đảo",
    "tiêu cực", "cảnh báo", "nguy hiểm", "rủi ro", "siết chặt",
    "xử lý nghiêm", "khởi tố", "bắt giam", "thanh tra", "vi phạm",
    "cứu", "sập", "bắt đáy", "hoảng loạn", "tụt dốc",
]
_POSITIVE_KW = [
    "tăng", "mua vào", "bứt phá", "tốt", "tích cực", "khuyến nghị",
    "hỗ trợ", "nới lỏng", "giảm lãi suất", "bơm tiền", "khuyến khích",
    "tăng trưởng", "kỳ vọng", "đột phá", "xuất sắc", "múc", "tím",
    "lên", "tăng mạnh", "vọt lên",
]

def _keyword_sentiment(text: str) -> float:
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


# ── Normalizers (Nhận Input là Dict) ─────────────────────────────────────────

def normalize_social_post(post_dict: Dict[str, Any], ticker_context: str = "") -> Dict[str, Any]:
    """
    Chuẩn hóa social post thành schema thống nhất.
    Input: dict hoặc dataclass RawSocialPost
    """
    # Nếu là dataclass, convert sang dict
    if hasattr(post_dict, '__dataclass_fields__'):
        post_dict = {
            'post_id': post_dict.post_id,
            'source': post_dict.source,
            'source_name': post_dict.source_name,
            'content_text': post_dict.content_text,
            'published_at': post_dict.published_at,
            'likes': post_dict.likes,
            'shares': post_dict.shares,
            'comments': post_dict.comments,
            'credibility': post_dict.credibility,
            'ticker_context': getattr(post_dict, 'ticker_context', ''),
            'url': getattr(post_dict, 'url', ''),
        }
    
    ctx = ticker_context or post_dict.get("ticker_context", "UNKNOWN")
    content = post_dict.get("content_text", "")
    sentiment_score = _keyword_sentiment(content)

    return {
        "comment_id":        post_dict.get("post_id"),
        "ticker":            ctx,
        "content_text":      content,
        "created_at":        post_dict.get("published_at"),
        "likes":             max(0, int(post_dict.get("likes", 0))),
        "shares":            max(0, int(post_dict.get("shares", 0))),
        "comments":          max(0, int(post_dict.get("comments", 0))),
        "timestamp_ingested": _now_iso(),
        "normalized":        True,
        "source_dataset":    f"facebook_{post_dict.get('source_name', 'unknown')}",
        "ticker_context":    ctx,
        "source":            post_dict.get("source"),
        "source_name":       post_dict.get("source_name"),
        "url":               post_dict.get("url"),
        "credibility":       post_dict.get("credibility", 0.55),
        "sentiment": {
            "label":      "positive" if sentiment_score > 0.1 else ("negative" if sentiment_score < -0.1 else "neutral"),
            "confidence": abs(sentiment_score),
        },
    }

def normalize_news_article(article_dict: Dict[str, Any], ticker_context: str = "") -> Dict[str, Any]:
    """
    Chuẩn hóa news article thành schema thống nhất.
    Input: dict hoặc dataclass RawNewsItem
    """
    # Nếu là dataclass, convert sang dict
    if hasattr(article_dict, '__dataclass_fields__'):
        article_dict = {
            'article_id': article_dict.article_id,
            'source': article_dict.source,
            'title': article_dict.title,
            'content_text': article_dict.content_text,
            'url': article_dict.url,
            'published_at': article_dict.published_at,
            'credibility': article_dict.credibility,
            'ticker_context': getattr(article_dict, 'ticker_context', ''),
        }
    
    ctx = ticker_context or article_dict.get("ticker_context", "UNKNOWN")
    content = article_dict.get("content_text", "")
    sentiment_score = _keyword_sentiment(content)

    return {
        "article_id":        article_dict.get("article_id"),
        "ticker_context":    ctx,
        "source":            article_dict.get("source"),
        "title":             article_dict.get("title"),
        "content_text":      content,
        "url":               article_dict.get("url"),
        "published_at":      article_dict.get("published_at"),
        "timestamp_ingested": _now_iso(),
        "credibility":       article_dict.get("credibility", 0.8),
        "source_type":       "news",
        "sentiment": {
            "label":      "positive" if sentiment_score > 0.1 else ("negative" if sentiment_score < -0.1 else "neutral"),
            "confidence": abs(sentiment_score),
        },
        "comment_id":    article_dict.get("article_id"),
        "ticker":        ctx,
        "likes":         0,
        "shares":        0,
        "comments":      0,
        "normalized":    True,
        "source_dataset": f"news_{article_dict.get('source')}",
    }

def normalize_policy_doc(doc_dict: Dict[str, Any], ticker_context: str = "") -> Dict[str, Any]:
    ctx = ticker_context or doc_dict.get("ticker_context", "POLICY")
    
    return {
        "doc_id":            doc_dict.get("doc_id"),
        "ticker_context":    ctx,
        "source":            "nhnn",
        "doc_type":          doc_dict.get("doc_type"),
        "title":             doc_dict.get("title"),
        "content_text":      doc_dict.get("content_text"),
        "url":               doc_dict.get("url"),
        "published_at":      doc_dict.get("published_at"),
        "timestamp_ingested": _now_iso(),
        "credibility":       1.0,
        "source_type":       "policy",
        "metadata": {
            "source_file":    doc_dict.get("doc_id"),
            "ticker_context": ctx,
            "publish_date":   doc_dict.get("published_at"),
            "document_type":  "policy_regulation",
            "chunk_index":    0,
            "doc_type":       doc_dict.get("doc_type"),
            "url":            doc_dict.get("url"),
        },
    }

# ── Batch normalizers ──────────────────────────────────────────────────────────

def normalize_social_batch(posts: List[Dict[str, Any]], ticker_context: str = "") -> List[Dict[str, Any]]:
    return [normalize_social_post(p, ticker_context) for p in posts]

def normalize_news_batch(articles: List[Dict[str, Any]], ticker_context: str = "") -> List[Dict[str, Any]]:
    return [normalize_news_article(a, ticker_context) for a in articles]

def normalize_policy_batch(docs: List[Dict[str, Any]], ticker_context: str = "") -> List[Dict[str, Any]]:
    return [normalize_policy_doc(d, ticker_context) for d in docs]

# Lưu ý: Vnstock dữ liệu cần normalize để phù hợp với Kafka schema

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