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

# Lưu ý: Vnstock hiện tại trong file stock_crawler đã trả về dữ liệu đúng chuẩn luôn nên có thể bỏ qua bước normalize này.