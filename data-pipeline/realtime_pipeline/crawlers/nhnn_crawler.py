# -*- coding: utf-8 -*-
"""
realtime_pipeline/crawlers/nhnn_crawler.py

Thu thap van ban hanh chinh tu Ngan hang Nha nuoc Viet Nam (sbv.gov.vn):
  - Thong tu, Quyet dinh, Chi thi, Thong bao chinh sach
  - Scrape trang list van ban + lay noi dung

Output: list[RawPolicyDoc] → ingest vao ChromaDB realtime collection
"""

import hashlib
import logging
import time
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

from realtime_pipeline.config import (
    NHNN_BASE_URL, HISTORY_START_DATE, HISTORY_END_DATE
)

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_TIMEOUT  = 20
_MAX_PAGES = 5    # So trang toi da moi loai van ban
_MAX_TEXT  = 6000 # ky tu toi da moi van ban


@dataclass
class RawPolicyDoc:
    doc_id:        str
    title:         str
    content_text:  str
    url:           str
    published_at:  str       # ISO 8601
    doc_type:      str       # "Thong tu" | "Quyet dinh" | ...
    ticker_context: str = "" # Set boi producer
    source:        str = "nhnn"
    credibility:   float = 1.0
    source_type:   str = "policy"


def _make_doc_id(url: str) -> str:
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    return f"nhnn_{digest}"


def _parse_vn_date(date_str: str) -> str:
    """
    Parse ngay Viet Nam (dd/mm/yyyy hoac yyyy-mm-dd) → ISO UTC.
    Fallback ve now neu khong parse duoc.
    """
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return datetime.now(timezone.utc).isoformat()


def _is_in_date_range(date_str: str) -> bool:
    try:
        dt    = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        start = datetime.fromisoformat(HISTORY_START_DATE + "T00:00:00+00:00")
        end   = datetime.fromisoformat(HISTORY_END_DATE   + "T23:59:59+00:00")
        return start <= dt <= end
    except Exception:
        return True


def _clean_text(raw: str) -> str:
    soup = BeautifulSoup(raw, "lxml")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text[:_MAX_TEXT]


def _fetch_doc_content(url: str) -> str:
    """Scrape noi dung trang chi tiet van ban."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "lxml")

        # Thu cac selector pho bien cua sbv.gov.vn
        for sel in [
            "div.article-body",
            "div.content-detail",
            "div.van-ban-content",
            "div.box-content",
            "article",
            "div#content",
        ]:
            node = soup.select_one(sel)
            if node:
                return _clean_text(str(node))

        # Fallback
        body = soup.find("body")
        return _clean_text(str(body)) if body else ""
    except Exception as e:
        logger.debug(f"Khong fetch duoc {url}: {e}")
        return ""


def crawl_nhnn_documents(doc_type: str = "Thong tu") -> List[RawPolicyDoc]:
    """
    Crawl danh sach van ban NHNN theo loai.
    Dung HTML scraping vi NHNN chua co RSS chinh thuc.
    
    Nhieu trang NHNN block bot, nen co retry va fallback graceful.
    """
    docs: List[RawPolicyDoc] = []

    # URL danh sach van ban NHNN (phu thuoc cau truc web hien tai)
    list_url = f"{NHNN_BASE_URL}/webcenter/portal/vi/menu/trangchu/vb"
    params = {
        "category":     doc_type,
        "fromDate":     HISTORY_START_DATE,
        "toDate":       HISTORY_END_DATE,
        "_afrLoop":     "123456789",
    }

    for page in range(1, _MAX_PAGES + 1):
        try:
            params["page"] = page
            url = list_url + "?" + urlencode(params, encoding="utf-8")
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.encoding = "utf-8"

            if resp.status_code != 200:
                logger.warning(f"NHNN HTTP {resp.status_code} trang {page}, bo qua")
                break

            soup = BeautifulSoup(resp.text, "lxml")

            # Tim tat ca link van ban trong trang
            links = soup.select("a[href*='/vb/']") or soup.select("table.result-list td a")
            if not links:
                logger.info(f"Khong con link van ban NHNN trang {page}, dung lai")
                break

            for link in links:
                try:
                    href  = link.get("href", "")
                    title = link.get_text(strip=True)
                    if not href or len(title) < 10:
                        continue

                    full_url = urljoin(NHNN_BASE_URL, href)

                    # Lay ngay ban hanh tu row tuong ung
                    row    = link.find_parent("tr")
                    date_td = row.find_all("td")[1] if row else None
                    date_raw = date_td.get_text(strip=True) if date_td else ""
                    pub_date  = _parse_vn_date(date_raw) if date_raw else datetime.now(timezone.utc).isoformat()

                    if not _is_in_date_range(pub_date):
                        continue

                    time.sleep(0.5)
                    content = _fetch_doc_content(full_url)
                    if not content or len(content) < 100:
                        content = title  # fallback chi dung title

                    doc_id = _make_doc_id(full_url)
                    docs.append(RawPolicyDoc(
                        doc_id       = doc_id,
                        title        = title,
                        content_text = f"{title}. {content}",
                        url          = full_url,
                        published_at = pub_date,
                        doc_type     = doc_type,
                        credibility  = 1.0,
                    ))

                except Exception as e:
                    logger.debug(f"Loi xu ly link NHNN: {e}")
                    continue

            time.sleep(1.0)

        except Exception as e:
            logger.error(f"Loi crawl NHNN trang {page}: {e}")
            break

    logger.info(f"[NHNN] {doc_type}: {len(docs)} van ban")
    return docs


def crawl_nhnn_all_types() -> List[RawPolicyDoc]:
    """Thu thap tat ca loai van ban NHNN."""
    from realtime_pipeline.config import NHNN_DOC_TYPES
    all_docs: List[RawPolicyDoc] = []
    for doc_type in NHNN_DOC_TYPES:
        try:
            docs = crawl_nhnn_documents(doc_type)
            all_docs.extend(docs)
        except Exception as e:
            logger.error(f"Loi crawl NHNN {doc_type}: {e}")
    logger.info(f"[NHNN] Tong {len(all_docs)} van ban")
    return all_docs
