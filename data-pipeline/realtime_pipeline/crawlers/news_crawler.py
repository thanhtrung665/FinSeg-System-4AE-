# -*- coding: utf-8 -*-
"""
realtime_pipeline/crawlers/news_crawler.py

Thu thap bai bao chinh thong tu:
  - CafeF:       cafef.vn   (RSS + scraping)
  - Vietstock:   vietstock.vn (RSS + scraping)
  - ChinhPhu:    xaydungchinhsach.chinhphu.vn (RSS)

Output: list[RawNewsItem] — chuan hoa de producer day vao Kafka.
"""

import hashlib
import logging
import time
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup

from realtime_pipeline.config import NEWS_SOURCES, HISTORY_START_DATE

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

_REQUEST_TIMEOUT = 15   # giay
_MAX_ARTICLE_LEN = 5000  # ky tu


@dataclass
class RawNewsItem:
    article_id:    str
    source:        str          # "cafef" | "vietstock" | "chinhphu"
    title:         str
    content_text:  str
    url:           str
    published_at:  str          # ISO 8601 UTC
    ticker_context: str = ""    # set boi producer khi biet context
    credibility:   float = 0.8
    source_type:   str = "news"


# ── Helper ─────────────────────────────────────────────────────────────────────

def _make_id(source: str, url: str) -> str:
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    return f"{source}_{digest}"


def _parse_feed_date(entry) -> str:
    """Lay ngay tu feedparser entry → ISO UTC string."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    return datetime.now(timezone.utc).isoformat()


def _is_after_history_start(date_str: str) -> bool:
    """Chi lay bai bao tu HISTORY_START_DATE tro di."""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        start = datetime.fromisoformat(HISTORY_START_DATE + "T00:00:00+00:00")
        return dt >= start
    except Exception:
        return True  # Neu khong parse duoc, lay het


def _is_after_start_date(date_str: str, start_date: str) -> bool:
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        start = datetime.fromisoformat(start_date + "T00:00:00+00:00")
        return dt >= start
    except Exception:
        return True


def _clean_html(raw: str) -> str:
    """Loai bo HTML tags, giu lai text thuan."""
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "lxml")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text[:_MAX_ARTICLE_LEN]


def _fetch_article_body(url: str, source: str) -> str:
    """
    Scrape noi dung bai bao tu URL.
    Moi nguon co CSS selector rieng.
    """
    selectors = {
        "cafef":    ["div.detail-content", "div.article-body", "div.maincontent"],
        "vietstock": ["div.article-content", "div.contentdetail", "div.news-detail"],
        "chinhphu": ["div.article-body", "div.content-detail", "article"],
    }
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "lxml")

        for sel in selectors.get(source, ["article", "div.content"]):
            node = soup.select_one(sel)
            if node:
                return _clean_html(str(node))

        # Fallback: lay toan bo body
        body = soup.find("body")
        return _clean_html(str(body)) if body else ""
    except Exception as e:
        logger.debug(f"Khong scrape duoc {url}: {e}")
        return ""


# ── Core crawl ─────────────────────────────────────────────────────────────────

def crawl_rss_source(source_name: str, source_cfg: dict) -> List[RawNewsItem]:
    """Crawl tat ca RSS URLs cua 1 nguon."""
    items: List[RawNewsItem] = []
    credibility = source_cfg.get("credibility", 0.8)

    for rss_url in source_cfg.get("rss_urls", []):
        try:
            feed = feedparser.parse(rss_url)
            logger.info(f"[{source_name}] RSS {rss_url} → {len(feed.entries)} entries")

            for entry in feed.entries:
                published_at = _parse_feed_date(entry)
                if not _is_after_history_start(published_at):
                    continue

                title   = getattr(entry, "title", "").strip()
                url     = getattr(entry, "link",  "").strip()
                summary = getattr(entry, "summary", "")

                if not title or not url:
                    continue

                # Lay body tu summary truoc, neu qua ngan thi scrape
                body = _clean_html(summary)
                if len(body) < 200:
                    time.sleep(0.3)
                    body = _fetch_article_body(url, source_name)
                    if not body:
                        body = _clean_html(summary)

                if len(body) < 50:
                    continue

                items.append(RawNewsItem(
                    article_id    = _make_id(source_name, url),
                    source        = source_name,
                    title         = title,
                    content_text  = f"{title}. {body}",
                    url           = url,
                    published_at  = published_at,
                    credibility   = credibility,
                    source_type   = "news",
                ))

            time.sleep(0.5)  # polite delay

        except Exception as e:
            logger.error(f"[{source_name}] Loi crawl {rss_url}: {e}")

    logger.info(f"[{source_name}] Tong cong {len(items)} articles")
    return items


def crawl_all_news() -> List[RawNewsItem]:
    """Thu thap tat ca nguon tin tuc chinh thong."""
    all_items: List[RawNewsItem] = []
    for source_name, source_cfg in NEWS_SOURCES.items():
        try:
            items = crawl_rss_source(source_name, source_cfg)
            all_items.extend(items)
        except Exception as e:
            logger.error(f"Loi crawl {source_name}: {e}")
    logger.info(f"Tong bai bao crawl duoc: {len(all_items)}")
    return all_items


def crawl_news_for_ticker(
    ticker: str,
    start_date: Optional[str] = None,
) -> List[RawNewsItem]:
    """
    Crawl tin tuc lien quan den 1 ma co phieu.
    Loc theo keyword trong title/content.
    """
    # Map ticker → cac tu khoa tim kiem
    ticker_keywords = {
        "SHB":     ["shb", "sai gon ha noi"],
        "VCB":     ["vcb", "vietcombank"],
        "TCB":     ["tcb", "techcombank"],
        "MBB":     ["mbb", "mb bank"],
        "VPB":     ["vpb", "vpbank"],
        "BID":     ["bid", "bidv"],
        "CTG":     ["ctg", "vietinbank"],
        "VNINDEX": ["vnindex", "vn-index", "chung khoan", "thi truong"],
    }

    keywords = ticker_keywords.get(ticker.upper(), [ticker.lower()])
    all_items = crawl_all_news()

    if start_date:
        all_items = [
            item for item in all_items
            if _is_after_start_date(item.published_at, start_date)
        ]

    filtered = []
    for item in all_items:
        text_lower = (item.title + " " + item.content_text).lower()
        if any(kw in text_lower for kw in keywords):
            item.ticker_context = ticker
            filtered.append(item)

    logger.info(f"[{ticker}] Loc duoc {len(filtered)}/{len(all_items)} bai bao lien quan")
    return filtered
