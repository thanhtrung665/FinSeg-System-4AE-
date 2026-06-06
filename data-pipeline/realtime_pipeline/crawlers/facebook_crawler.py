# -*- coding: utf-8 -*-
"""
realtime_pipeline/crawlers/facebook_crawler.py

Thu thap bai viet va comment tu Facebook Groups/Pages:
  - https://www.facebook.com/groups/congdongchungkhoanchinhthuc
  - https://www.facebook.com/cafechungkhoanek

QUAN TRONG:
  Facebook khong co API public. Cac phuong phap thu thap:
  1. Selenium + Chrome (chay tren GPU server co man hinh)
  2. Playwright headless (khuyen nghi cho server)

  Tren moi truong khong co browser (CI/CD), module se chay o che do STUB
  tra ve du lieu mau de pipeline khong bi dung.

  De chay that: dat bien moi truong
    FB_EMAIL=your@email.com
    FB_PASSWORD=your_password
  hoac dung cookie da export.

GPU server: pip install playwright && playwright install chromium
"""

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Load .env truoc khi doc credentials ──────────────────────────────────────
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=str(_PIPELINE_ROOT / ".env"))
except Exception:
    pass

# ── Credentials (doc sau khi .env da load) ────────────────────────────────────
_FB_EMAIL    = os.getenv("FB_EMAIL", "")
_FB_PASSWORD = os.getenv("FB_PASSWORD", "")
_FB_COOKIE_RAW = os.getenv("FB_COOKIE_FILE", "")

# Resolve cookie path — thu tuong doi va tuyet doi
def _resolve_cookie_path(raw: str) -> str:
    if not raw:
        return ""
    p = Path(raw)
    if p.is_absolute() and p.exists():
        return str(p)
    # Thu relative tu data-pipeline/
    p2 = _PIPELINE_ROOT / raw
    if p2.exists():
        return str(p2)
    # Thu relative tu realtime_pipeline/crawlers/
    p3 = Path(__file__).parent / raw
    if p3.exists():
        return str(p3)
    # Thu cac ten file pho bien trong data-pipeline/
    for fname in [raw, Path(raw).name]:
        for root in [_PIPELINE_ROOT, _PIPELINE_ROOT / "realtime_pipeline"]:
            candidate = root / fname
            if candidate.exists():
                return str(candidate)
    return raw   # Tra ve nguyen neu khong tim thay (se bao loi ro rang sau)

_FB_COOKIE = _resolve_cookie_path(_FB_COOKIE_RAW)

def _playwright_available() -> bool:
    try:
        from playwright.sync_api import sync_playwright
        return True
    except ImportError:
        return False

_CRAWL_READY = bool(_FB_EMAIL and _FB_PASSWORD) or bool(_FB_COOKIE and Path(_FB_COOKIE).exists())


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


# ── Selenium crawler ──────────────────────────────────────────────────────────

def _crawl_with_selenium(target: dict, max_posts: int = 50) -> List[RawSocialPost]:
    """
    Crawl Facebook voi Selenium.
    Yeu cau: selenium, Chrome/Chromium driver.
    """
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException

    posts: List[RawSocialPost] = []

    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    chrome_opts.add_experimental_option("excludeSwitches", ["enable-automation"])

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_opts)
        wait   = WebDriverWait(driver, 15)

        # Dang nhap
        driver.get("https://www.facebook.com/login")
        time.sleep(2)

        # Nhap email
        email_input = wait.until(EC.presence_of_element_located((By.ID, "email")))
        email_input.send_keys(_FB_EMAIL)

        # Nhap mat khau
        pass_input = driver.find_element(By.ID, "pass")
        pass_input.send_keys(_FB_PASSWORD)

        # Bam dang nhap
        driver.find_element(By.NAME, "login").click()
        time.sleep(4)

        # Kiem tra dang nhap thanh cong
        if "login" in driver.current_url.lower():
            logger.warning("Facebook dang nhap that bai — kiem tra email/mat khau")
            return []

        # Vao trang group/page
        driver.get(target["url"])
        time.sleep(3)

        # Scroll de load bai viet
        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

        # Tim cac bai viet
        post_elements = driver.find_elements(
            By.CSS_SELECTOR,
            'div[data-ad-preview="message"], '
            'div[class*="userContent"], '
            'div[data-testid="post_message"]'
        )

        for elem in post_elements[:max_posts]:
            try:
                text = elem.text.strip()
                if not text or len(text) < 20:
                    continue

                # Lay reaction count (likes)
                likes = 0
                try:
                    reaction_elem = elem.find_element(
                        By.XPATH, './/ancestor::div[contains(@class,"story")]'
                        '//span[contains(@aria-label,"reaction")]'
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
                logger.debug(f"Loi doc post: {e}")
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
    """
    Crawl Facebook voi Playwright (nhe hon Selenium, hop hon cho server).
    Ho tro 2 che do:
      - Cookie file (Netscape format hoac JSON) → khong can dang nhap lai
      - Email + Password → dang nhap binh thuong
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

    posts: List[RawSocialPost] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="vi-VN",
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        try:
            # ── Buoc 1: Xac thuc ──────────────────────────────────────────
            authed = False

            # Uu tien cookie file (khong bi Facebook detect la automation)
            if _FB_COOKIE and Path(_FB_COOKIE).exists():
                logger.info(f"Dung cookie file: {_FB_COOKIE}")
                raw_cookie = Path(_FB_COOKIE).read_text(encoding="utf-8")

                # Parse Netscape cookie format (tu extension EditThisCookie, etc.)
                cookies = []
                for line in raw_cookie.splitlines():
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split("\t")
                    if len(parts) >= 7:
                        try:
                            cookies.append({
                                "name":   parts[5],
                                "value":  parts[6],
                                "domain": parts[0].lstrip("."),
                                "path":   parts[2],
                                "secure": parts[3].lower() == "true",
                            })
                        except IndexError:
                            continue

                # Thu parse JSON (format khac)
                if not cookies:
                    try:
                        import json
                        json_cookies = json.loads(raw_cookie)
                        for c in json_cookies:
                            cookies.append({
                                "name":   c.get("name", ""),
                                "value":  c.get("value", ""),
                                "domain": c.get("domain", ".facebook.com").lstrip("."),
                                "path":   c.get("path", "/"),
                                "secure": c.get("secure", True),
                            })
                    except Exception:
                        pass

                if cookies:
                    ctx.add_cookies(cookies)
                    logger.info(f"Da them {len(cookies)} cookies")
                    page.goto("https://www.facebook.com", timeout=15000)
                    page.wait_for_timeout(2000)
                    authed = "login" not in page.url
                    if authed:
                        logger.info("Cookie auth thanh cong!")

            # Fallback: dang nhap bang email/pass
            if not authed and _FB_EMAIL and _FB_PASSWORD:
                logger.info("Dang nhap bang email/password...")
                page.goto("https://www.facebook.com/", timeout=15000)
                page.wait_for_timeout(2000)

                # Xu ly cookie consent popup (neu co)
                for btn_sel in [
                    'button[data-cookiebanner="accept_button"]',
                    'button[title="Allow all cookies"]',
                    'button[title="Cho phep tat ca cookie"]',
                    '[data-testid="cookie-policy-manage-dialog-accept-button"]',
                ]:
                    try:
                        if page.locator(btn_sel).is_visible(timeout=2000):
                            page.click(btn_sel)
                            page.wait_for_timeout(1000)
                            break
                    except Exception:
                        pass

                # Vao trang login
                page.goto("https://www.facebook.com/login", timeout=15000)
                page.wait_for_timeout(2000)

                # Xu ly consent popup lan nua neu xuat hien
                for btn_sel in [
                    'button[data-cookiebanner="accept_button"]',
                    '[aria-label="Dong"]',
                    '[aria-label="Close"]',
                ]:
                    try:
                        if page.locator(btn_sel).is_visible(timeout=1500):
                            page.click(btn_sel)
                            page.wait_for_timeout(500)
                    except Exception:
                        pass

                # Nhap credentials
                try:
                    page.fill("#email", _FB_EMAIL, timeout=10000)
                    page.fill("#pass",  _FB_PASSWORD, timeout=5000)
                    page.click('[name="login"]')
                    page.wait_for_timeout(6000)
                    authed = "login" not in page.url.lower() and "checkpoint" not in page.url.lower()
                    if authed:
                        logger.info("Login thanh cong!")
                    else:
                        logger.error(f"Login that bai — URL: {page.url}")
                        browser.close()
                        return []
                except Exception as e:
                    logger.error(f"Loi khi nhap credentials: {e}")
                    browser.close()
                    return []

            if not authed:
                logger.error("Khong the xac thuc Facebook")
                browser.close()
                return []

            # ── Buoc 2: Vao trang target ──────────────────────────────────
            logger.info(f"Dang vao: {target['url']}")
            page.goto(target["url"], timeout=20000)
            page.wait_for_timeout(3000)

            # Scroll de load bai viet
            for _ in range(4):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)

            # ── Buoc 3: Lay bai viet ──────────────────────────────────────
            # CafeF chung khoan Facebook dung cac selector nay
            post_selectors = [
                'div[data-ad-preview="message"]',
                'div[class*="userContent"]',
                'div[data-testid="post_message"]',
                'div[class*="xdj266r"]',   # Facebook new UI 2024
                'div[class*="x1iorvi4"]',
            ]

            post_texts = []
            for sel in post_selectors:
                elems = page.locator(sel).all()
                for el in elems:
                    try:
                        txt = el.inner_text()
                        if txt and len(txt.strip()) > 20:
                            post_texts.append(txt.strip())
                    except Exception:
                        continue
                if post_texts:
                    break

            # Fallback: lay tat ca text block dai
            if not post_texts:
                all_divs = page.locator("div").all()
                for div in all_divs[:200]:
                    try:
                        txt = div.inner_text()
                        if 50 < len(txt.strip()) < 2000:
                            post_texts.append(txt.strip())
                    except Exception:
                        continue

            # De-duplicate va tao RawSocialPost
            seen_hash = set()
            for txt in post_texts[:max_posts]:
                h = hashlib.md5(txt[:60].encode()).hexdigest()
                if h in seen_hash:
                    continue
                seen_hash.add(h)
                posts.append(RawSocialPost(
                    post_id      = _make_post_id(target["name"], h),
                    source       = target["type"],
                    source_name  = target["name"],
                    content_text = txt,
                    published_at = datetime.now(timezone.utc).isoformat(),
                    credibility  = target.get("credibility", 0.55),
                    url          = target["url"],
                ))

        except PlaywrightTimeout as e:
            logger.error(f"Timeout khi crawl {target['name']}: {e}")
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
    "Lo ngai ve no xau ngan hang tang trong Q2/2025",
    "NHNN giam lai suat dieu hanh, tac dong tich cuc len nganh ngan hang",
    "Rut tien hang loat tai mot so ngan hang nho, thi truong lo lang",
    "SHB bao lai quy 2 vuot ke hoach 15%, co the xet mua",
    "Nen hoang loang voi co phieu ngan hang trong boi canh hien tai",
]

def _generate_stub_posts(target: dict) -> List[RawSocialPost]:
    """Sinh du lieu stub khi khong crawl duoc - im lang o muc DEBUG."""
    logger.debug(
        f"[FB/{target['name']}] Stub mode "
        "(Set FB_EMAIL + FB_PASSWORD de crawl that)"
    )
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
    """
    Thu thap bai viet tu 1 Facebook target.
    Tu dong chon: Playwright > Selenium > Stub.
    """
    if not _CRAWL_READY:
        return _generate_stub_posts(target)

    # Uu tien Playwright (nhe hon, hop hon cho GPU server)
    if _playwright_available():
        try:
            posts = _crawl_with_playwright(target, max_posts)
            if posts:
                return posts
        except Exception as e:
            logger.warning(f"Playwright that bai, thu Selenium: {e}")

    # Fallback sang Selenium
    try:
        posts = _crawl_with_selenium(target, max_posts)
        if posts:
            return posts
    except Exception as e:
        logger.warning(f"Selenium that bai: {e}")

    # Final fallback
    return _generate_stub_posts(target)


def crawl_all_facebook() -> List[RawSocialPost]:
    """Thu thap tat ca Facebook targets."""
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


def crawl_facebook_for_ticker(ticker: str) -> List[RawSocialPost]:
    """Crawl va loc bai viet lien quan den 1 ma co phieu."""
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

    filtered = []
    for post in all_posts:
        text_lower = post.content_text.lower()
        if any(kw in text_lower for kw in keywords):
            post.ticker_context = ticker
            filtered.append(post)
        elif not keywords:
            post.ticker_context = ticker
            filtered.append(post)

    # Neu qua it, lay het khong loc (de co du lieu cho VMSI)
    if len(filtered) < 5:
        for post in all_posts:
            post.ticker_context = ticker
        return all_posts

    logger.info(f"[FB/{ticker}] {len(filtered)} posts lien quan")
    return filtered
