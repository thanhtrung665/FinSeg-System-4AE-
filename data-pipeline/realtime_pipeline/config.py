# -*- coding: utf-8 -*-
"""
realtime_pipeline/config.py
Config tap trung cho toan bo realtime pipeline.
Mo rong PipelineConfig cu, them cac crawl settings.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Resolve path ──────────────────────────────────────────────────────────────
_RT_ROOT = Path(__file__).resolve().parent          # realtime_pipeline/
_PIPELINE_ROOT = _RT_ROOT.parent                    # data-pipeline/

# Them data-pipeline/ vao sys.path de import data_pipeline_ingestion
sys.path.insert(0, str(_PIPELINE_ROOT))

_ENV_FILE = _PIPELINE_ROOT / ".env"
load_dotenv(dotenv_path=str(_ENV_FILE))

# ── Import settings goc ───────────────────────────────────────────────────────
from data_pipeline_ingestion.config import settings as base_settings

# ── Realtime settings ─────────────────────────────────────────────────────────

# Kafka topics moi cho realtime
KAFKA_TOPIC_NEWS          = "realtime_news"         # Bao chinh thong (CafeF, Vietstock, ChinhPhu)
KAFKA_TOPIC_SOCIAL        = "realtime_social"       # Facebook posts/comments
KAFKA_TOPIC_MARKET        = "realtime_market"       # Gia co phieu (vnstock)
KAFKA_TOPIC_POLICY        = "realtime_policy"       # Van ban NHNN

# Crawl interval (giay)
CRAWL_INTERVAL_SECONDS    = int(os.getenv("CRAWL_INTERVAL_SECONDS", "1800"))  # 30 phut

# VMSI cycle interval
VMSI_CYCLE_SECONDS        = int(os.getenv("VMSI_CYCLE_SECONDS", "1800"))      # 30 phut

# Date range lay du lieu lich su: Thang 4 → Thang 6 hien tai
HISTORY_START_DATE        = os.getenv("HISTORY_START_DATE", "2025-03-01")
HISTORY_END_DATE          = os.getenv("HISTORY_END_DATE", "2026-06-01")

# Nguon tin tuc
NEWS_SOURCES = {
    "cafef": {
        "rss_urls": [
            # RSS van hoat dong (kiem tra 2025-06)
            "https://cafef.vn/thi-truong-chung-khoan.rss",
            "https://cafef.vn/bat-dong-san.rss",
            "https://cafef.vn/vi-mo-dau-tu.rss",
            "https://cafef.vn/doanh-nghiep.rss",
        ],
        "scrape_urls": [
            # Trang listing HTML .chn — lay bai moi nhat qua h3 a selector
            "https://cafef.vn/tai-chinh-ngan-hang.chn",
            "https://cafef.vn/thi-truong-chung-khoan.chn",
            "https://cafef.vn/doanh-nghiep.chn",
        ],
        "listing_selector": "h3 a",
        "base_url": "https://cafef.vn",
        "credibility": 0.85,
    },
    "vietstock": {
        "rss_urls": [],
        "scrape_urls": [
            "https://vietstock.vn/chung-khoan.htm",
            "https://vietstock.vn/tai-chinh.htm",
        ],
        "listing_selector": "h3 a, h4 a, .title a",
        "base_url": "https://vietstock.vn",
        "credibility": 0.85,
    },
    "chinhphu": {
        "rss_urls": [
            # RSS chinh xac (kiem tra 2025-06)
            "https://xaydungchinhsach.chinhphu.vn/rss",
        ],
        "scrape_urls": [],
        "listing_selector": "h3 a",
        "base_url": "https://xaydungchinhsach.chinhphu.vn",
        "credibility": 1.0,
    },
}

# NHNN document API endpoint (tra cuu van ban)
NHNN_BASE_URL          = "https://www.sbv.gov.vn"
NHNN_SEARCH_URL        = "https://www.sbv.gov.vn/webcenter/portal/vi/menu/trangchu/vb"
NHNN_DOC_TYPES         = ["Thong tu", "Quyet dinh", "Chi thi", "Thong bao", "Cong van"]

# Facebook Groups/Pages can crawl (dung Selenium / Playwright)
FACEBOOK_TARGETS = [
    {
        "name":       "congdongchungkhoan",
        "url":        "https://www.facebook.com/groups/congdongchungkhoanchinhthuc",
        "type":       "group",
        "credibility": 0.55,
    },
    {
        "name":       "cafechungkhoan",
        "url":        "https://www.facebook.com/cafechungkhoanek",
        "type":       "page",
        "credibility": 0.65,
    },
]

# Stock symbols mac dinh theo ticker context
TICKER_MAP = {
    "SHB":     "SHB",
    "SCB":     "VNINDEX",   # SCB khong niem yet, dung VNINDEX lam proxy
    "VCB":     "VCB",
    "TCB":     "TCB",
    "MBB":     "MBB",
    "VPB":     "VPB",
    "BID":     "BID",
    "CTG":     "CTG",
    "HDB":     "HDB",
    "LPB":     "LPB"
    "VNINDEX": "VNINDEX",
}

# ChromaDB collection cho realtime (tach biet voi collection demo)
CHROMA_REALTIME_COLLECTION = os.getenv(
    "CHROMA_REALTIME_COLLECTION", "realtime_policies"
)

# Re-export base settings de cac module con dung
KAFKA_BROKER   = base_settings.KAFKA_BROKER
CHROMADB_MODE  = base_settings.CHROMADB_MODE

def get_chroma_client():
    return base_settings.get_chroma_client()
