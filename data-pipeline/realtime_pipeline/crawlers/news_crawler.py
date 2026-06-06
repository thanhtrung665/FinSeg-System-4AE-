import hashlib
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urljoin

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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
_TIMEOUT     = 12
_MAX_LEN     = 5000
_MIN_CONTENT = 80   # bo qua bai co noi dung qua ngan


@dataclass
class RawNewsItem:
    article_id:    str
    source:        str
    title:         str
    content_text:  str
    url:           str
    published_at:  str
    ticker_context: str  = ""
    credibility:   float = 0.8
    source_type:   str   = "news"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_id(source: str, url: str) -> str:
    return f"{source}_{hashlib.md5(url.encode()).hexdigest()[:12]}"


def _parse_feed_date(entry) -> str:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    return datetime.now(timezone.utc).isoformat()


def _is_recent(date_str: str) -> bool:
    """Chi lay bai tu HISTORY_START_DATE tro di."""
    try:
        dt    = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        start = datetime.fromisoformat(HISTORY_START_DATE + "T00:00:00+00:00")
        return dt >= start
    except Exception:
        return True


def _clean_html(raw: str) -> str:
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "lxml")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text[:_MAX_LEN]


def _scrape_article(url: str) -> str:
    """Scrape noi dung chi tiet 1 bai bao."""
    selectors_by_domain = {
        "cafef.vn":      ["div.detail-content", "div.article-body", "article"],
        "vietstock.vn":  ["div.article-content", "div.contentdetail", "div.news-detail", "article"],
        "chinhphu.vn":   ["div.article-body", "div.content-detail", "article"],
    }
    try:
        r = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "lxml")

        domain = url.split("/")[2].replace("www.", "")
        selectors = selectors_by_domain.get(domain, ["article", "div.content"])

        for sel in selectors:
            node = soup.select_one(sel)
            if node:
                return _clean_html(str(node))

        body = soup.find("body")
        return _clean_html(str(body)) if body else ""
    except Exception as e:
        logger.debug(f"Scrape {url}: {e}")
        return ""


# ── RSS crawler ────────────────────────────────────────────────────────────────

def crawl_rss_source(source_name: str, source_cfg: dict) -> List[RawNewsItem]:
    """Crawl tat ca RSS URLs cua 1 nguon."""
    items: List[RawNewsItem] = []
    credibility = source_cfg.get("credibility", 0.8)

    for rss_url in source_cfg.get("rss_urls", []):
        try:
            r    = requests.get(rss_url, headers=_HEADERS, timeout=_TIMEOUT)
            feed = feedparser.parse(r.text)
            logger.info(f"[{source_name}] RSS {rss_url} → {len(feed.entries)} entries")

            for entry in feed.entries:
                published_at = _parse_feed_date(entry)
                if not _is_recent(published_at):
                    continue

                title = getattr(entry, "title", "").strip()
                url   = getattr(entry, "link",  "").strip()
                if not title or not url:
                    continue

                # Thu dung summary truoc, neu ngan thi scrape
                body = _clean_html(getattr(entry, "summary", ""))
                if len(body) < _MIN_CONTENT:
                    time.sleep(0.3)
                    body = _scrape_article(url) or body

                if len(body) < 30:
                    continue

                items.append(RawNewsItem(
                    article_id   = _make_id(source_name, url),
                    source       = source_name,
                    title        = title,
                    content_text = f"{title}. {body}",
                    url          = url,
                    published_at = published_at,
                    credibility  = credibility,
                ))

            time.sleep(0.5)

        except Exception as e:
            logger.error(f"[{source_name}] RSS {rss_url}: {e}")

    return items


# ── Scraping crawler (cho Vietstock khong co RSS) ──────────────────────────────

def crawl_scrape_source(source_name: str, source_cfg: dict) -> List[RawNewsItem]:
    """
    Scrape danh sach bai bao tu trang listing HTML.
    Ho tro CafeF (.chn dung h3 a) va Vietstock (.htm).
    """
    items: List[RawNewsItem] = []
    credibility       = source_cfg.get("credibility", 0.8)
    base_url          = source_cfg.get("base_url", "")
    listing_selector  = source_cfg.get("listing_selector", "h3 a")

    for page_url in source_cfg.get("scrape_urls", []):
        try:
            r = requests.get(page_url, headers=_HEADERS, timeout=_TIMEOUT)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "lxml")

            # Lay link bai viet dung selector da cau hinh
            links_and_titles: List[tuple] = []
            for sel in listing_selector.split(","):
                sel = sel.strip()
                for a in soup.select(sel):
                    href  = a.get("href", "").strip()
                    title = a.get_text(strip=True)
                    if not href or len(title) < 15:
                        continue
                    full_url = urljoin(base_url, href) if not href.startswith("http") else href
                    # Chi lay link cung domain
                    if base_url.split("/")[2] in full_url:
                        links_and_titles.append((full_url, title))

            # De-duplicate
            seen = set()
            unique = []
            for url, title in links_and_titles:
                if url not in seen:
                    seen.add(url)
                    unique.append((url, title))

            logger.info(f"[{source_name}] Scrape {page_url} → {len(unique)} links")

            count = 0
            for art_url, title in unique[:25]:  # Toi da 25 bai / page
                try:
                    # Voi listing .chn (CafeF), title da du chat luong
                    # Chi scrape full content khi title qua ngan
                    if len(title) >= 40:
                        content = title  # Dung title, tiep kiem thoi gian
                    else:
                        time.sleep(0.3)
                        content = _scrape_article(art_url) or title

                    items.append(RawNewsItem(
                        article_id   = _make_id(source_name, art_url),
                        source       = source_name,
                        title        = title,
                        content_text = f"{title}. {content}",
                        url          = art_url,
                        published_at = datetime.now(timezone.utc).isoformat(),
                        credibility  = credibility,
                    ))
                    count += 1

                except Exception as e:
                    logger.debug(f"Skip {art_url}: {e}")

            logger.info(f"[{source_name}] Scrape hoan tat: {count}/{len(unique)} bai tu {page_url}")
            time.sleep(1.0)

        except Exception as e:
            logger.error(f"[{source_name}] Scrape {page_url}: {e}")

    return items


# ── Public API ─────────────────────────────────────────────────────────────────

def crawl_rss_and_scrape_source(source_name: str, source_cfg: dict) -> List[RawNewsItem]:
    """Crawl ca RSS va scrape cho 1 nguon (tu dong chon phuong phap)."""
    items = crawl_rss_source(source_name, source_cfg)
    items += crawl_scrape_source(source_name, source_cfg)
    logger.info(f"[{source_name}] Tong: {len(items)} articles")
    return items


def crawl_all_news() -> List[RawNewsItem]:
    """Thu thap tat ca nguon tin tuc chinh thong."""
    all_items: List[RawNewsItem] = []
    for source_name, source_cfg in NEWS_SOURCES.items():
        try:
            items = crawl_rss_and_scrape_source(source_name, source_cfg)
            all_items.extend(items)
        except Exception as e:
            logger.error(f"Loi crawl {source_name}: {e}")
    logger.info(f"Tong bai bao: {len(all_items)}")
    return all_items


def crawl_news_for_ticker(ticker: str) -> List[RawNewsItem]:
    """Crawl va loc bai bao lien quan den 1 ma co phieu."""
    ticker_keywords = {
        "SHB":     ["shb", "sai gon ha noi", "ngan hang shb"],
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

    filtered = []
    for item in all_items:
        text_lower = (item.title + " " + item.content_text).lower()
        if any(kw in text_lower for kw in keywords):
            item.ticker_context = ticker
            filtered.append(item)

    logger.info(f"[{ticker}] Loc duoc {len(filtered)}/{len(all_items)} bai lien quan")
    return filtered
