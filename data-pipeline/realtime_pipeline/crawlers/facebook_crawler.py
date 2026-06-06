# -*- coding: utf-8 -*-
"""
realtime_pipeline/crawlers/facebook_crawler.py

Thu thap bai viet va comment tu Facebook Groups/Pages.
Sử dụng Cookie từ tiện ích "Get cookies.txt LOCALLY".
"""

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)

# ── Kiem tra moi truong ───────────────────────────────────────────────────────
_FB_EMAIL    = os.getenv("FB_EMAIL", "")
_FB_PASSWORD = os.getenv("FB_PASSWORD", "")
_FB_COOKIE   = os.getenv("FB_COOKIE_FILE", "")  # VD: crawlers/facebook.com_cookies.txt

def _playwright_available() -> bool:
    try:
        import playwright
        return True
    except ImportError:
        return False

_CRAWL_READY = bool(_FB_EMAIL and _FB_PASSWORD) or bool(_FB_COOKIE)


@dataclass
class RawSocialPost:
    post_id:       str
    source:        str           # "facebook_group" | "facebook_page"
    source_name:   str           # "congdongchungkhoan" | "cafechungkhoan"
    content_text:  str
    published_at:  str           # ISO 8601 UTC
    likes:         int = 0
    shares:        int = 0
    comments:      int = 0
    ticker_context: str = ""
    credibility:   float = 0.55
    source_type:   str = "social"
    url:           str = ""


def _make_post_id(source: str, raw_id: str) -> str:
    digest = hashlib.md5(raw_id.encode("utf-8")).hexdigest()[:12]
    return f"fb_{source}_{digest}"


# ── Hàm phân tích file Cookies.txt (Netscape format) ─────────────────────────
def _parse_netscape_cookies(file_path: str) -> List[dict]:
    """Chuyển đổi file cookies.txt sang định dạng JSON cho Playwright."""
    cookies = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Bỏ qua dòng trống hoặc ghi chú (trừ HttpOnly)
                if not line or (line.startswith('#') and not line.startswith('#HttpOnly_')):
                    continue
                
                # Bỏ tiền tố HttpOnly nếu có
                if line.startswith('#HttpOnly_'):
                    line = line[10:]
                    
                parts = line.split('\t')
                # Đảm bảo dòng có đủ 7 trường thông tin của chuẩn Netscape
                if len(parts) >= 7:
                    cookies.append({
                        "domain": parts[0],
                        "path": parts[2],
                        "secure": parts[3].upper() == "TRUE",
                        "name": parts[5],
                        "value": parts[6]
                    })
        return cookies
    except Exception as e:
        logger.error(f"Lỗi đọc file cookie: {e}")
        return []


def _is_after_start_date(published_at: str, start_date: str) -> bool:
    try:
        dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        start = datetime.fromisoformat(start_date + 'T00:00:00+00:00')
        return dt >= start
    except Exception:
        return True


# ── Selenium crawler ──────────────────────────────────────────────────────────

def _crawl_with_selenium(target: dict, max_posts: int = 50) -> List[RawSocialPost]:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    posts: List[RawSocialPost] = []
    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_opts)
        wait   = WebDriverWait(driver, 15)

        # Ưu tiên load cookie nếu có
        if _FB_COOKIE and os.path.exists(_FB_COOKIE):
            driver.get("https://www.facebook.com/404") # Truy cập domain trước khi set cookie
            cookies = _parse_netscape_cookies(_FB_COOKIE)
            for cookie in cookies:
                driver.add_cookie(cookie)
            driver.get(target["url"])
            time.sleep(3)
        else:
            driver.get("https://www.facebook.com/login")
            time.sleep(2)
            wait.until(EC.presence_of_element_located((By.ID, "email"))).send_keys(_FB_EMAIL)
            driver.find_element(By.ID, "pass").send_keys(_FB_PASSWORD)
            driver.find_element(By.NAME, "login").click()
            time.sleep(4)
            if "login" in driver.current_url.lower():
                logger.warning("Facebook dang nhap that bai — kiem tra email/mat khau")
                return []
            driver.get(target["url"])
            time.sleep(3)

        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

        post_elements = driver.find_elements(
            By.CSS_SELECTOR,
            'div[data-ad-preview="message"], div[class*="userContent"], div[data-testid="post_message"]'
        )

        for elem in post_elements[:max_posts]:
            try:
                text = elem.text.strip()
                if not text or len(text) < 20:
                    continue

                likes = 0
                try:
                    reaction_elem = elem.find_element(
                        By.XPATH, './/ancestor::div[contains(@class,"story")]//span[contains(@aria-label,"reaction")]'
                    )
                    likes_text = re.sub(r"\D", "", reaction_elem.text)
                    likes = int(likes_text) if likes_text else 0
                except Exception:
                    pass

                raw_id  = hashlib.md5(text[:50].encode()).hexdigest()
                post_id = _make_post_id(target["name"], raw_id)

                posts.append(RawSocialPost(
                    post_id      = post_id,
                    source       = target["type"],
                    source_name  = target["name"],
                    content_text = text,
                    published_at = datetime.now(timezone.utc).isoformat(),
                    likes        = likes,
                    credibility  = target.get("credibility", 0.55),
                ))
            except Exception as e:
                continue

    except Exception as e:
        logger.error(f"Loi Selenium crawl {target['name']}: {e}")
    finally:
        if driver:
            driver.quit()

    logger.info(f"[FB/{target['name']}] Crawl duoc {len(posts)} posts (Selenium)")
    return posts


# ── Playwright crawler (khuyen nghi cho GPU server) ──────────────────────────

def _crawl_with_playwright(target: dict, max_posts: int = 50) -> List[RawSocialPost]:
    from playwright.sync_api import sync_playwright

    posts: List[RawSocialPost] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="vi-VN",
        )
        page = ctx.new_page()

        try:
            # 1. Nạp Cookie thông minh từ file .txt
            if _FB_COOKIE and os.path.exists(_FB_COOKIE):
                cookies = _parse_netscape_cookies(_FB_COOKIE)
                if cookies:
                    ctx.add_cookies(cookies)
                    logger.info("Nạp cookie thành công, truy cập Facebook...")
                    page.goto(target["url"], timeout=30000)
                else:
                    logger.warning("File cookie trống hoặc không hợp lệ, chuyển sang đăng nhập chay.")
            
            # 2. Hoặc đăng nhập chay nếu không có cookie
            if not _FB_COOKIE or not os.path.exists(_FB_COOKIE) or not cookies:
                page.goto("https://www.facebook.com/login", timeout=15000)
                page.fill("#email", _FB_EMAIL)
                page.fill("#pass", _FB_PASSWORD)
                page.click('[name="login"]')
                page.wait_for_timeout(4000)

                if "login" in page.url:
                    logger.warning("Playwright: Dang nhap Facebook that bai")
                    return []
                page.goto(target["url"], timeout=20000)

            page.wait_for_timeout(3000)

            # Scroll de load noi dung
            for _ in range(5):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)

            # Lay noi dung bai viet
            post_texts = page.eval_on_selector_all(
                'div[data-ad-preview="message"], div[class*="userContent"]',
                "elements => elements.map(el => el.innerText)"
            )

            for text in post_texts[:max_posts]:
                text = text.strip()
                if not text or len(text) < 20:
                    continue

                raw_id  = hashlib.md5(text[:50].encode()).hexdigest()
                post_id = _make_post_id(target["name"], raw_id)

                posts.append(RawSocialPost(
                    post_id      = post_id,
                    source       = target["type"],
                    source_name  = target["name"],
                    content_text = text,
                    published_at = datetime.now(timezone.utc).isoformat(),
                    credibility  = target.get("credibility", 0.55),
                ))

        except Exception as e:
            logger.error(f"Loi Playwright crawl {target['name']}: {e}")
        finally:
            browser.close()

    logger.info(f"[FB/{target['name']}] Crawl duoc {len(posts)} posts (Playwright)")
    return posts


# ── Stub data (khi khong co browser / chua cau hinh FB credentials) ──────────

_STUB_POSTS = [
    "SHB hom nay tang manh, nen mua vao khong moi nguoi?",
    "Gia SHB dang o vung ho tro tot, co the bat day",
    "Canh bao: tin don NHNN siet chat tin dung BDS anh huong nhieu co phieu ngan hang",
    "VNIndex dang test vung 1250, can theo doi phan ung",
    "Co phieu ngan hang dang duoc khoi ngoai mua rong, tin hieu tot",
]

def _generate_stub_posts(target: dict) -> List[RawSocialPost]:
    logger.debug(f"[FB/{target['name']}] Stub mode (Set FB_COOKIE_FILE de crawl that)")
    posts = []
    for i, text in enumerate(_STUB_POSTS):
        post_id = _make_post_id(target["name"], f"stub_{i}")
        posts.append(RawSocialPost(
            post_id      = post_id,
            source       = target["type"],
            source_name  = target["name"],
            content_text = text,
            published_at = datetime.now(timezone.utc).isoformat(),
            likes        = (i + 1) * 15,
            shares       = i * 3,
            comments     = (i + 1) * 5,
            credibility  = target.get("credibility", 0.55),
        ))
    return posts


# ── Public API ────────────────────────────────────────────────────────────────

def crawl_facebook_target(target: dict, max_posts: int = 50) -> List[RawSocialPost]:
    if not _CRAWL_READY:
        return _generate_stub_posts(target)

    if _playwright_available():
        try:
            posts = _crawl_with_playwright(target, max_posts)
            if posts:
                return posts
        except Exception as e:
            logger.warning(f"Playwright that bai, thu Selenium: {e}")

    try:
        posts = _crawl_with_selenium(target, max_posts)
        if posts:
            return posts
    except Exception as e:
        logger.warning(f"Selenium that bai: {e}")

    return _generate_stub_posts(target)


def crawl_all_facebook() -> List[RawSocialPost]:
    from realtime_pipeline.config import FACEBOOK_TARGETS
    all_posts: List[RawSocialPost] = []
    for target in FACEBOOK_TARGETS:
        try:
            posts = crawl_facebook_target(target)
            all_posts.extend(posts)
        except Exception as e:
            logger.error(f"Loi crawl {target['name']}: {e}")
    logger.info(f"Tong Facebook posts: {len(all_posts)}")
    return all_posts


def crawl_facebook_for_ticker(ticker: str, start_date: str = None) -> List[RawSocialPost]:
    ticker_keywords = {
        "SHB": ["shb", "sai gon ha noi"],
        "VCB": ["vcb", "vietcombank"],
        "TCB": ["tcb", "techcombank"],
        "MBB": ["mbb", "mb bank"],
        "VPB": ["vpb", "vpbank"],
        "BID": ["bidv", "bid"],
        "CTG": ["vietinbank", "ctg"],
        "VNINDEX": ["vnindex", "vni", "thi truong", "chung khoan"],
    }
    keywords = ticker_keywords.get(ticker.upper(), [ticker.lower()])
    all_posts = crawl_all_facebook()

    if start_date:
        all_posts = [
            post for post in all_posts
            if _is_after_start_date(post.published_at, start_date)
        ]

    filtered = []
    for post in all_posts:
        text_lower = post.content_text.lower()
        if any(kw in text_lower for kw in keywords):
            post.ticker_context = ticker
            filtered.append(post)
        elif not keywords:
            post.ticker_context = ticker
            filtered.append(post)

    if len(filtered) < 5:
        for post in all_posts:
            post.ticker_context = ticker
        return all_posts

    logger.info(f"[FB/{ticker}] {len(filtered)} posts lien quan")
    return filtered
