# -*- coding: utf-8 -*-
"""
realtime_pipeline/crawlers/nhnn_crawler.py

Thu thập văn bản hành chính từ Ngân hàng Nhà nước Việt Nam (sbv.gov.vn):
  - Thông tư, Quyết định, Chỉ thị
  - Trả về danh sách Dictionary để chuẩn bị cho Kafka/ChromaDB.
"""

import hashlib
import logging
import time
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_TIMEOUT  = 20
_MAX_PAGES = 5    
_MAX_TEXT  = 6000 

class NHNNCrawler:
    """Class Crawler độc lập, không phụ thuộc file config cứng"""
    def __init__(self, start_date="2024-04-01", end_date=None):
        self.base_url = "https://www.sbv.gov.vn"
        self.start_date = start_date
        # Mặc định lấy đến ngày hiện tại nếu không cung cấp end_date
        self.end_date = end_date if end_date else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        self.doc_types = ["Thông tư", "Quyết định", "Chỉ thị"]

    def _make_doc_id(self, url: str) -> str:
        digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
        return f"nhnn_{digest}"

    def _parse_vn_date(self, date_str: str) -> str:
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                continue
        return datetime.now(timezone.utc).isoformat()

    def _is_in_date_range(self, date_str: str) -> bool:
        try:
            dt    = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            start = datetime.fromisoformat(self.start_date + "T00:00:00+00:00")
            end   = datetime.fromisoformat(self.end_date   + "T23:59:59+00:00")
            return start <= dt <= end
        except Exception:
            return True

    def _clean_text(self, raw: str) -> str:
        if not raw: return ""
        soup = BeautifulSoup(raw, "lxml")
        text = soup.get_text(separator=" ", strip=True)
        return re.sub(r"\s+", " ", text)[:_MAX_TEXT]

    def _fetch_doc_content(self, url: str) -> str:
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")

            for sel in [
                "div.article-body", "div.content-detail", 
                "div.van-ban-content", "div.box-content", 
                "article", "div#content"
            ]:
                node = soup.select_one(sel)
                if node: return self._clean_text(str(node))

            body = soup.find("body")
            return self._clean_text(str(body)) if body else ""
        except Exception as e:
            logger.debug(f"Không fetch được nội dung chi tiết {url}: {e}")
            return ""

    def crawl_nhnn_documents(self, doc_type: str) -> List[Dict]:
        """Crawl danh sách văn bản theo loại."""
        docs: List[Dict] = []
        
        # Lưu ý: Cần điều tra cấu trúc thật của sbv.gov.vn, URL này có thể thay đổi
        list_url = f"{self.base_url}/webcenter/portal/vi/menu/trangchu/vb"
        params = {
            "category":     doc_type,
            "fromDate":     self.start_date,
            "toDate":       self.end_date,
            "_afrLoop":     "123456789", # Giữ nguyên tham số bắt buộc của hệ thống cổng thông tin
        }

        for page in range(1, _MAX_PAGES + 1):
            try:
                params["page"] = page
                url = list_url + "?" + urlencode(params, encoding="utf-8")
                resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
                resp.encoding = "utf-8"

                if resp.status_code != 200:
                    logger.warning(f"NHNN HTTP {resp.status_code} trang {page}, dừng tìm kiếm loại {doc_type}.")
                    break

                soup = BeautifulSoup(resp.text, "lxml")
                links = soup.select("a[href*='/vb/']") or soup.select("table.result-list td a")
                
                if not links:
                    logger.info(f"Không còn link văn bản NHNN ở trang {page}, dừng tìm kiếm loại {doc_type}.")
                    break

                for link in links:
                    try:
                        href  = link.get("href", "")
                        title = link.get_text(strip=True)
                        if not href or len(title) < 10:
                            continue

                        full_url = urljoin(self.base_url, href)
                        
                        # Xử lý an toàn khi tìm ngày tháng
                        row = link.find_parent("tr")
                        tds = row.find_all("td") if row else []
                        date_raw = tds[1].get_text(strip=True) if len(tds) > 1 else ""
                        pub_date = self._parse_vn_date(date_raw) if date_raw else datetime.now(timezone.utc).isoformat()

                        if not self._is_in_date_range(pub_date):
                            continue

                        time.sleep(0.5) # Tránh bị block IP
                        content = self._fetch_doc_content(full_url)
                        if not content or len(content) < 100:
                            content = title

                        docs.append({
                            "doc_id": self._make_doc_id(full_url),
                            "title": title,
                            "content_text": f"{title}. {content}",
                            "url": full_url,
                            "published_at": pub_date,
                            "doc_type": doc_type,
                            "credibility": 1.0,
                            "source_type": "policy",
                            "source": "nhnn"
                        })

                    except Exception as e:
                        logger.debug(f"Lỗi xử lý link {href}: {e}")
                        continue
                
                time.sleep(1.0) # Đợi trước khi qua trang mới
                
            except Exception as e:
                logger.error(f"Lỗi crawl NHNN loại {doc_type} trang {page}: {e}")
                break

        logger.info(f"[NHNN] Đã quét xong {doc_type}: thu được {len(docs)} văn bản hợp lệ.")
        return docs

    def crawl_all(self) -> List[Dict]:
        """Thu thập tất cả các loại văn bản đã định nghĩa."""
        all_docs: List[Dict] = []
        for doc_type in self.doc_types:
            try:
                docs = self.crawl_nhnn_documents(doc_type)
                all_docs.extend(docs)
            except Exception as e:
                logger.error(f"Lỗi crawl NHNN toàn cục với {doc_type}: {e}")
        
        logger.info(f"✅ Hoàn tất quét NHNN. Tổng thu thập: {len(all_docs)} văn bản.")
        return all_docs


@dataclass
class RawPolicyDoc:
    doc_id: str
    title: str
    content_text: str
    url: str
    published_at: str
    doc_type: str
    credibility: float = 1.0
    source_type: str = "policy"
    source: str = "nhnn"


def crawl_nhnn_all_types(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    crawler = NHNNCrawler(start_date=start_date or "2024-04-01", end_date=end_date)
    return crawler.crawl_all()

# --- Cách sử dụng file này sau này ---
# crawler = NHNNCrawler(start_date="2026-04-01")
# data = crawler.crawl_all()
# for item in data: producer.send(item)