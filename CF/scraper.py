"""
Bleems.com full scraper — Cloudflare R2 edition
-------------------------------------------------
Scrapes all shops (grouped by type: Flowers, Confections, Gifts, …),
their items, and their reviews, then uploads partitioned CSVs and images
to Cloudflare R2 (S3-compatible API).

R2 structure (identical partition layout to the AWS S3 version):
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/shops.csv
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/items.csv
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/reviews.csv
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/images/{shop-name}/logo/logo.jpg
  <bucket>/bleems-data/year=2026/month=02/day=23/flowers/images/{shop-name}/products/{product-id}.jpg
  <bucket>/bleems-data/year=2026/month=02/day=23/confections/...
  ...

Environment variables (set via GitHub Actions secrets):
  CF_R2_ACCESS_KEY_ID      Cloudflare R2 access key
  CF_R2_SECRET_ACCESS_KEY  Cloudflare R2 secret key
  CF_R2_ENDPOINT_URL       https://<account-id>.r2.cloudflarestorage.com
  CF_R2_BUCKET_NAME        R2 bucket name
"""

import difflib
import os
import re
import json
import time
import logging
from io import StringIO, BytesIO
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ──────────────────────────────────────────────────────────────────────────────
# Selector registry  (shared with the root-level scraper)
# ──────────────────────────────────────────────────────────────────────────────
_SELECTORS_FILE = Path(__file__).parent.parent / "selectors.json"
try:
    with open(_SELECTORS_FILE, encoding="utf-8") as _f:
        SEL: dict = json.load(_f)
except FileNotFoundError:
    raise FileNotFoundError(
        f"selectors.json not found at {_SELECTORS_FILE}. "
        "This file must sit one level above CF/scraper.py (i.e. at the repo root)."
    )

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL        = "https://www.bleems.com"
COUNTRY         = "kw"

# ── Cloudflare R2 credentials (explicit — no fallback credential chain) ───────
CF_R2_ACCESS_KEY    = os.environ.get("CF_R2_ACCESS_KEY_ID")
CF_R2_SECRET_KEY    = os.environ.get("CF_R2_SECRET_ACCESS_KEY")
CF_R2_ENDPOINT_URL  = os.environ.get("CF_R2_ENDPOINT_URL")
S3_BUCKET           = os.environ.get("CF_R2_BUCKET_NAME")   # R2 bucket name
S3_FOLDER           = "bleems-data"                          # top-level prefix inside bucket

_now       = datetime.now(timezone.utc)
TODAY      = _now.strftime("%Y-%m-%d")
S3_YEAR    = _now.strftime("%Y")
S3_MONTH   = _now.strftime("%m")
S3_DAY     = _now.strftime("%d")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.bleems.com/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Polite delay between requests (seconds)
REQUEST_DELAY = 1.5


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _get(url: str, **kwargs) -> requests.Response:
    """GET with retry (up to 3 times)."""
    for attempt in range(1, 4):
        try:
            resp = SESSION.get(url, timeout=30, **kwargs)
            resp.encoding = 'utf-8'
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            log.warning(f"Attempt {attempt}/3 failed for {url}: {exc}")
            if attempt < 3:
                time.sleep(3 * attempt)
    raise RuntimeError(f"All retries exhausted for {url}")


def _select_safe(soup, css: str, context: str = ""):
    """
    select_one wrapper: when nothing is found, logs a difflib hint showing the
    closest live CSS class names — so you know what the selector likely changed to.
    """
    result = soup.select_one(css)
    if result is None:
        live_classes = sorted({c for el in soup.find_all(class_=True) for c in el.get("class", [])})
        class_parts  = re.findall(r"\.(\w[\w-]*)", css)
        id_parts     = re.findall(r"#(\w[\w-]*)",  css)
        if class_parts or id_parts:
            target      = class_parts[0] if class_parts else id_parts[0]
            suggestions = difflib.get_close_matches(target, live_classes, n=3, cutoff=0.55)
            hint = f" → closest live classes: {suggestions}" if suggestions else " → no close match on this page"
            ctx  = f" ({context})" if context else ""
            log.warning(f"  [selector] '{css}' returned nothing{ctx}{hint}")
    return result


def _select_safe_all(soup, css: str, context: str = "") -> list:
    """
    select wrapper: when the list is empty, logs a difflib hint showing the
    closest live CSS class names — so you know what the selector likely changed to.
    """
    results = soup.select(css)
    if not results:
        live_classes = sorted({c for el in soup.find_all(class_=True) for c in el.get("class", [])})
        class_parts  = re.findall(r"\.(\w[\w-]*)", css)
        id_parts     = re.findall(r"#(\w[\w-]*)",  css)
        if class_parts or id_parts:
            target      = class_parts[0] if class_parts else id_parts[0]
            suggestions = difflib.get_close_matches(target, live_classes, n=3, cutoff=0.55)
            hint = f" → closest live classes: {suggestions}" if suggestions else " → no close match on this page"
            ctx  = f" ({context})" if context else ""
            log.warning(f"  [selector] '{css}' returned nothing{ctx}{hint}")
    return results


def _width_to_stars(style: str) -> float | None:
    """Convert CSS width% to a 1–5 star rating (20 % = 1 star)."""
    m = re.search(r"width\s*:\s*(\d+(?:\.\d+)?)%", style)
    if m:
        return round(float(m.group(1)) / 20, 1)
    return None


def _parse_reviewer(raw: str):
    """
    Parse strings like:
      'Fatma L on 11/12/2025'  → ('Fatma L',    '11/12/2025')
      '17/11/2024'             → ('',            '17/11/2024')
    """
    on_match   = re.match(r"^(.+?)\s+on\s+(\d{2}/\d{2}/\d{4})$", raw)
    date_match = re.match(r"^(\d{2}/\d{2}/\d{4})$", raw)
    if on_match:
        return on_match.group(1).strip(), on_match.group(2).strip()
    if date_match:
        return "", date_match.group(1)
    return raw.strip(), ""


# ──────────────────────────────────────────────────────────────────────────────
# Shop list
# ──────────────────────────────────────────────────────────────────────────────
def fetch_all_shops() -> list[dict]:
    """
    Parse https://www.bleems.com/kw/shops and return a list of shop dicts.
    Each shop dict contains: name, type, rating, ratings_count, slug, url, logo_url.
    """
    url = f"{BASE_URL}/{COUNTRY}/shops"
    log.info(f"Fetching shop list from {url}")
    html = _get(url).text
    soup = BeautifulSoup(html, "html.parser")

    def _parse_shops(use_data_attr: bool) -> list[dict]:
        result = []
        for el in _select_safe_all(soup, SEL["shop_list"]["shop_item"], "shops listing page"):
            href     = el.get("href", "")
            name_div = el.select_one(SEL["shop_list"]["shop_name"])
            name     = (name_div.text.strip() if name_div else el.get("data-name", "")).strip()

            shop_url = ""
            slug = ""
            onclick = el.get("onclick", "")
            m = re.search(r"onShopClicked\('([^']+)'", onclick)
            if m:
                shop_url = m.group(1)
                slug = shop_url.split("/shop/")[-1].rstrip("/")
            elif "/shop/" in href and "javascript:" not in href:
                slug = href.split("/shop/")[-1].rstrip("/")
                shop_url = f"{BASE_URL}/{COUNTRY}/shop/{slug}"

            img      = el.select_one("img")
            type_div = el.select_one(SEL["shop_list"]["shop_type"])

            if use_data_attr:
                type_text = el.get("data-type", "Other").strip()
            else:
                type_text = (
                    type_div.text.strip() if (type_div and type_div.text.strip())
                    else el.get("data-type", "Other").strip()
                )

            result.append({
                "name":          name,
                "type":          type_text.title(),
                "rating":        el.get("data-rating", ""),
                "ratings_count": el.get("data-count", ""),
                "slug":          slug,
                "url":           shop_url,
                "logo_url":      img.get("src", "") if img else "",
            })
        return result

    shops = _parse_shops(use_data_attr=False)
    unique_types = sorted({s["type"] for s in shops})
    log.info(f"Found {len(shops)} shops. Types from HTML text: {unique_types}")

    if len(unique_types) <= 1:
        log.warning(
            "Only one type found in visible text — falling back to data-type attribute."
        )
        shops = _parse_shops(use_data_attr=True)
        unique_types = sorted({s["type"] for s in shops})
        log.info(f"Types from data-type attribute: {unique_types}")

    return shops


# ──────────────────────────────────────────────────────────────────────────────
# Items
# ──────────────────────────────────────────────────────────────────────────────

_HTML_ENTITIES = {
    "&#x1F382;": "🎂", "&#x1F319;": "🌙", "&#x1F381;": "🎁",
    "&#x1F338;": "🌸", "&#x1F490;": "💐", "&#x1F36C;": "🍬",
    "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
}


def _js_obj_to_json(raw: str) -> str:
    """
    Convert a JavaScript object literal (single-quoted keys/values,
    decodeHTMLString() calls) to valid JSON that json.loads() can parse.
    """
    raw = re.sub(
        r"decodeHTMLString\(['\"]([^'\"]*?)['\"]\)",
        lambda m: json.dumps(m.group(1)),
        raw,
    )

    for ent, char in _HTML_ENTITIES.items():
        raw = raw.replace(ent, char)

    result = []
    i = 0
    in_double = False
    while i < len(raw):
        ch = raw[i]
        if ch == '"':
            in_double = not in_double
            result.append(ch)
        elif ch == "'" and not in_double:
            j = i + 1
            buf = []
            while j < len(raw):
                c = raw[j]
                if c == "'":
                    break
                if c == '"':
                    buf.append('\\"')
                else:
                    buf.append(c)
                j += 1
            result.append('"')
            result.extend(buf)
            result.append('"')
            i = j + 1
            continue
        else:
            result.append(ch)
        i += 1
    raw = "".join(result)

    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    return raw


def _extract_track_json(html: str) -> dict | None:
    """
    Extract the single `var trackJson = { … };` from a product page and
    return it as a parsed dict, or None if not found / unparseable.
    """
    m = re.search(r"var\s+trackJson\s*=\s*(\{.*?\})\s*;", html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(_js_obj_to_json(m.group(1)))
    except Exception as exc:
        log.debug(f"trackJson parse error: {exc}")
        return None


def _collect_product_urls(shop_html: str, shop_slug: str) -> list[tuple[str, str]]:
    """
    Return a list of (product_url, div_target_key) tuples from a shop page.
    Filters to products belonging to this shop and excludes sponsored ads.
    """
    soup = BeautifulSoup(shop_html, "html.parser")
    seen, pairs = set(), []

    actual_slug = None
    first_shop_link = _select_safe(soup, SEL["product_card"]["shop_link"], "shop page")
    if first_shop_link:
        href = first_shop_link.get("href", "")
        if "/shop/" in href:
            actual_slug = href.split("/shop/")[-1].strip("/")
            log.debug(f"    Extracted actual shop slug from page: '{actual_slug}'")

    if not actual_slug:
        actual_slug = shop_slug
        log.debug(f"    Using provided shop slug: '{shop_slug}'")

    for card in _select_safe_all(soup, SEL["product_card"]["card"], "shop page"):
        div = card.select_one(SEL["product_card"]["head"])
        if not div:
            continue

        target = div.get("data-content-target", "").strip().lstrip("/")
        if not target:
            continue

        if "?source=ad" in target or "&source=ad" in target:
            continue

        if actual_slug and actual_slug not in target:
            continue

        if target not in seen:
            seen.add(target)
            pairs.append((f"{BASE_URL}/{COUNTRY}/{target}", target))

    return pairs


def _row_from_track_json(data: dict, shop: dict) -> dict:
    """Build a flat item CSV row from a parsed trackJson dict."""
    flavors = data.get("flavor", [])
    colors  = data.get("color",  [])

    actual_shop_name = data.get("shop_name", shop["name"])
    actual_category  = data.get("category",  shop["type"])

    price        = data.get("product_price") or data.get("price_per", "")
    product_name = data.get("product_name")  or data.get("product", "")

    return {
        "shop_name":    actual_shop_name,
        "shop_type":    actual_category,
        "product_id":   data.get("content_id", ""),
        "product_name": product_name.strip() if product_name else "",
        "category":     actual_category,
        "brand":        data.get("brand", ""),
        "price":        str(price),
        "currency":     data.get("currency", "KWD"),
        "occasion":     data.get("occasion", ""),
        "product_type": data.get("product_type", ""),
        "sub_category": data.get("sub_category", ""),
        "flavors":      ", ".join(flavors) if isinstance(flavors, list) else str(flavors),
        "colors":       ", ".join(colors)  if isinstance(colors,  list) else str(colors),
        "product_url":  data.get("product_url", ""),
        "image_url":    data.get("product_image_url", ""),
    }


def fetch_shop_items(shop_html: str, shop: dict, s3: "boto3.client") -> list[dict]:
    """
    Fetch all products for a shop by visiting each individual product page.
    Downloads and uploads product images to Cloudflare R2.
    Falls back to minimal row (product_id + image only) if a page fails.
    """
    shop_slug = shop.get("slug", "")
    pairs     = _collect_product_urls(shop_html, shop_slug)
    shop_soup = BeautifulSoup(shop_html, "html.parser")

    div_lookup: dict[str, object] = {}
    for div in shop_soup.select(SEL["product_card"]["head"]):
        key = div.get("data-content-target", "").strip().lstrip("/")
        div_lookup[key] = div

    items = []
    log.info(f"    Fetching {len(pairs)} product pages …")

    clean_shop_name  = re.sub(r'[^\w\-]', '_', shop['name'])
    shop_type_folder = shop['type']

    for prod_url, target_key in pairs:
        time.sleep(REQUEST_DELAY)
        data = None
        try:
            resp = SESSION.get(prod_url, timeout=30)
            resp.encoding = 'utf-8'
            if resp.status_code == 200:
                data = _extract_track_json(resp.text)

                if not data:
                    log.debug(f"    No trackJson, parsing HTML for {prod_url}")
                    prod_soup = BeautifulSoup(resp.text, "html.parser")

                    title_el  = _select_safe(prod_soup, SEL["product_detail"]["title"],          prod_url)
                    price_el  = prod_soup.select_one(SEL["product_detail"]["price"])
                    shop_link = prod_soup.select_one(SEL["product_detail"]["shop_link"])
                    desc_el   = prod_soup.select_one(SEL["product_detail"]["description"])
                    img_el    = prod_soup.select_one(SEL["product_detail"]["image"])

                    if title_el:
                        data = {
                            "product_name": title_el.text.strip(),
                            "shop_name":    shop_link.text.strip() if shop_link else shop["name"],
                            "price_per":    price_el.text.replace("KWD", "").strip() if price_el else "",
                            "category":     shop["type"],
                            "product_url":  prod_url,
                            "product_image_url": img_el.get("src", "") if img_el else "",
                        }
                        pid_input = prod_soup.select_one(SEL["product_detail"]["product_id_input"])
                        if pid_input:
                            data["content_id"] = pid_input.get("value", "")

        except requests.RequestException as exc:
            log.debug(f"    Product fetch error {prod_url}: {exc}")

        if data:
            item = _row_from_track_json(data, shop)
        else:
            div = div_lookup.get(target_key)
            if div:
                pid      = div.get("data-content-name", "").replace("Product_", "")
                parent   = div.parent if div.parent else div
                name_el  = parent.select_one(SEL["product_card_fallback"]["item_name"])
                price_el = parent.select_one(SEL["product_card_fallback"]["item_price"])
                shop_el  = parent.select_one(SEL["product_card_fallback"]["shop_name"])
                img_el   = div.select_one("img")

                item = {
                    "shop_name":    shop_el.text.replace("by", "").strip() if shop_el else shop["name"],
                    "shop_type":    shop["type"],
                    "product_id":   pid,
                    "product_name": name_el.text.strip() if name_el else "",
                    "category":     shop["type"],
                    "brand":        shop["name"],
                    "price":        price_el.text.replace("KWD", "").strip() if price_el else "",
                    "currency":     "KWD",
                    "occasion":     "",
                    "product_type": "",
                    "sub_category": "",
                    "flavors":      "",
                    "colors":       "",
                    "product_url":  prod_url,
                    "image_url":    img_el.get("src", "") if img_el else "",
                }
            else:
                item = {
                    "shop_name":    shop["name"],
                    "shop_type":    shop["type"],
                    "product_id":   "",
                    "product_name": "",
                    "category":     shop["type"],
                    "brand":        shop["name"],
                    "price":        "",
                    "currency":     "KWD",
                    "occasion":     "",
                    "product_type": "",
                    "sub_category": "",
                    "flavors":      "",
                    "colors":       "",
                    "product_url":  prod_url,
                    "image_url":    "",
                }

        # Download and upload product image to R2
        item["r2_image_path"] = ""
        if item.get("image_url"):
            product_id = item.get("product_id", "unknown")
            ext = "jpg"
            if "." in item["image_url"]:
                ext = item["image_url"].split(".")[-1].split("?")[0][:4]

            r2_image_path = f"{S3_FOLDER}/year={S3_YEAR}/month={S3_MONTH}/day={S3_DAY}/{shop_type_folder}/images/{clean_shop_name}/products/{product_id}.{ext}"
            uploaded_path = upload_image_to_r2(item["image_url"], r2_image_path, s3)
            if uploaded_path:
                item["r2_image_path"] = uploaded_path

        items.append(item)

    return items


# ──────────────────────────────────────────────────────────────────────────────
# Reviews
# ──────────────────────────────────────────────────────────────────────────────

DEBUG_HTML = os.environ.get("DEBUG_HTML", "0") == "1"
_debug_dumped = False


def _make_review_row(shop: dict, text: str, raw_name: str, style: str) -> dict:
    reviewer_name, review_date = _parse_reviewer(raw_name.strip())
    star_rating = _width_to_stars(style)
    return {
        "shop_name":     shop["name"],
        "shop_type":     shop["type"],
        "reviewer_name": reviewer_name,
        "review_date":   review_date,
        "review_text":   text.strip(),
        "star_rating":   star_rating,
        "scraped_date":  TODAY,
    }


def _parse_reviews_from_html(html: str, shop: dict) -> list[dict]:
    """
    Two-strategy review parser.

    Strategy A — lxml soup (handles invalid HTML like <li> inside <div>).
    Strategy B — regex on raw HTML (fallback).
    """
    rows = []

    try:
        if isinstance(html, str):
            html_bytes = html.encode('utf-8')
        else:
            html_bytes = html
        soup = BeautifulSoup(html_bytes, "lxml", from_encoding="utf-8")
    except Exception:
        if isinstance(html, str):
            html_bytes = html.encode('utf-8')
        else:
            html_bytes = html
        soup = BeautifulSoup(html_bytes, "html.parser", from_encoding="utf-8")

    for el in soup.find_all(class_="li-reviews"):
        text_el   = el.find(class_="dv-reviews-text")
        name_el   = el.find(class_="dv-reviews-name")
        rating_el = el.find(class_="rating-on")

        rows.append(_make_review_row(
            shop,
            text_el.get_text() if text_el else "",
            name_el.get_text() if name_el else "",
            rating_el.get("style", "") if rating_el else "",
        ))

    if rows:
        return rows

    block_pat  = re.compile(
        r'class=["\']li-reviews["\'].*?(?=class=["\']li-reviews["\']|</ul|</div\s*id=["\']dv_reviews)',
        re.DOTALL,
    )
    text_pat   = re.compile(r'class=["\']dv-reviews-text["\'][^>]*>\s*(.*?)\s*</div', re.DOTALL)
    name_pat   = re.compile(r'class=["\']dv-reviews-name["\'][^>]*>\s*(.*?)\s*</div', re.DOTALL)
    rating_pat = re.compile(r'class=["\']rating-on["\']\s+style=["\']([^"\']+)["\']', re.DOTALL)

    for block in block_pat.finditer(html):
        segment  = block.group(0)
        text_m   = text_pat.search(segment)
        name_m   = name_pat.search(segment)
        rating_m = rating_pat.search(segment)
        rows.append(_make_review_row(
            shop,
            re.sub(r"<[^>]+>", "", text_m.group(1))   if text_m   else "",
            re.sub(r"<[^>]+>", "", name_m.group(1))   if name_m   else "",
            rating_m.group(1)                          if rating_m else "",
        ))

    return rows


def _parse_reviews_from_soup(soup: BeautifulSoup, shop: dict) -> list[dict]:
    """Legacy wrapper kept for compatibility with AJAX response parsing."""
    rows = []
    for el in soup.find_all(class_="li-reviews"):
        text_el   = el.find(class_="dv-reviews-text")
        name_el   = el.find(class_="dv-reviews-name")
        rating_el = el.find(class_="rating-on")
        rows.append(_make_review_row(
            shop,
            text_el.get_text()  if text_el   else "",
            name_el.get_text()  if name_el   else "",
            rating_el.get("style", "") if rating_el else "",
        ))
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Per-shop fetch
# ──────────────────────────────────────────────────────────────────────────────
def _get_csrf_token(html: str) -> str:
    """Extract ASP.NET RequestVerificationToken from a page's hidden input."""
    for tag in re.findall(r'<input[^>]+>', html):
        if '__RequestVerificationToken' in tag:
            m = re.search(r'value=["\']([^"\']+)["\']', tag)
            if m:
                return m.group(1)
    return ""


def _extract_shop_link(html: str, slug: str) -> str:
    """Find the shopLink value the page's JavaScript uses for the reviews API."""
    m = re.search(r'shopLink\s*=\s*["\']([^"\']+)["\']', html)
    if m:
        return m.group(1)

    m = re.search(r'data-shop-link=["\']([^"\']+)["\']', html)
    if m:
        return m.group(1)

    return f"/{COUNTRY}/shop/{slug}"


def fetch_reviews_for_shop(shop_slug: str, shop: dict, page_html: str) -> list[dict]:
    """
    Load ALL reviews via the AJAX endpoint used by the site.
    Paginates automatically until canLoad=false.
    """
    REVIEWS_URL = f"{BASE_URL}/{COUNTRY}/ItemsList?handler=LoadReviews"
    shop_url    = f"{BASE_URL}/{COUNTRY}/shop/{shop_slug}"

    rev_session = requests.Session()
    rev_session.headers.update(HEADERS)

    try:
        fresh_resp = rev_session.get(shop_url, timeout=30)
        fresh_resp.encoding = 'utf-8'
        fresh_html = fresh_resp.text
    except requests.RequestException as exc:
        log.warning(f"    Could not fetch shop page for CSRF: {exc}")
        return []

    csrf_token = _get_csrf_token(fresh_html)
    if not csrf_token:
        log.warning(f"    No CSRF token found for {shop['name']} – reviews skipped")
        return []

    log.info(f"    shopLink={shop_slug!r} (bare slug)  CSRF={csrf_token[:12]}…")

    get_headers = {
        "X-Requested-With":         "XMLHttpRequest",
        "RequestVerificationToken": csrf_token,
        "Accept":                   "application/json, text/javascript, */*; q=0.01",
        "Referer":                  shop_url,
    }

    all_rows: list[dict] = []
    page_no = 1

    while True:
        params = {
            "shopLink": shop_slug,
            "pageNo":   str(page_no),
            "pageSize": "20",
        }
        if page_no == 1:
            log.info(f"    GET {REVIEWS_URL}?shopLink={shop_slug}&pageNo=1&pageSize=20")
        try:
            resp = rev_session.get(
                REVIEWS_URL, params=params, headers=get_headers, timeout=30
            )
            if not resp.ok:
                log.warning(
                    f"    Reviews HTTP {resp.status_code} (page {page_no}):\n"
                    f"      Response headers: {dict(resp.headers)}\n"
                    f"      Body (first 400): {resp.content.decode('utf-8', errors='ignore')[:400].strip()}"
                )
                break
        except requests.RequestException as exc:
            log.warning(f"    Reviews request failed (page {page_no}): {exc}")
            break

        try:
            text = resp.content.decode('utf-8')
        except UnicodeDecodeError:
            text = resp.content.decode('utf-8', errors='replace')

        log.info(f"    Reviews GET {page_no}: status={resp.status_code} len={len(text)}")

        try:
            j        = json.loads(text)
            fragment = j.get("html", "")
            can_load = j.get("canLoad", False)
        except (json.JSONDecodeError, ValueError):
            log.warning(f"    Reviews page {page_no} not JSON — treating as HTML fragment")
            fragment = text
            can_load = False

        rows = _parse_reviews_from_html(fragment, shop)
        all_rows.extend(rows)
        log.debug(f"    Reviews page {page_no}: {len(rows)} rows (canLoad={can_load})")

        if not can_load or not rows:
            break

        page_no += 1
        time.sleep(REQUEST_DELAY)

    return all_rows


def fetch_shop_data(shop: dict, s3: "boto3.client") -> tuple[list[dict], list[dict], dict]:
    """
    Fetch a single shop page.
    Returns (items, reviews, enriched_shop_dict).
    Downloads and uploads shop logo and product images to Cloudflare R2.
    """
    url = f"{BASE_URL}/{COUNTRY}/shop/{shop['slug']}"
    try:
        resp = _get(url)
    except RuntimeError as exc:
        log.error(f"Skipping {shop['name']}: {exc}")
        return [], [], shop

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    rating_span = _select_safe(soup, SEL["shop_ratings"]["ratings_container"], shop["name"])
    if rating_span:
        rating_on = rating_span.select_one(SEL["shop_ratings"]["rating_stars"])
        if rating_on:
            shop["rating"] = _width_to_stars(rating_on.get("style", ""))
        count_el = rating_span.select_one(SEL["shop_ratings"]["ratings_count"])
        if count_el:
            m = re.search(r"\d+", count_el.text)
            if m:
                shop["ratings_count"] = int(m.group())

    # ── Download and upload shop logo to R2 ───────────────────────────────────
    shop["r2_image_path"] = ""
    if shop.get("logo_url"):
        clean_shop_name  = re.sub(r'[^\w\-]', '_', shop['name'])
        shop_type_folder = shop['type']

        ext = "jpg"
        if "." in shop["logo_url"]:
            ext = shop["logo_url"].split(".")[-1].split("?")[0][:4]

        r2_logo_path  = f"{S3_FOLDER}/year={S3_YEAR}/month={S3_MONTH}/day={S3_DAY}/{shop_type_folder}/images/{clean_shop_name}/logo/logo.{ext}"
        uploaded_path = upload_image_to_r2(shop["logo_url"], r2_logo_path, s3)
        if uploaded_path:
            shop["r2_image_path"] = uploaded_path
            log.debug(f"    Logo uploaded: {uploaded_path}")

    items = fetch_shop_items(html, shop, s3)

    global _debug_dumped
    if DEBUG_HTML and not _debug_dumped:
        dump_path = "debug_shop.html"
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"    DEBUG: raw HTML saved to {dump_path}")
        _debug_dumped = True

    reviews = _parse_reviews_from_html(html, shop)
    log.info(f"    inline reviews found: {len(reviews)}")

    if not reviews:
        shop_slug = shop.get("slug", "")
        reviews   = fetch_reviews_for_shop(shop_slug, shop, html)
        if not reviews:
            log.warning(f"    0 reviews for {shop['name']}")
        else:
            log.info(f"    reviews via AJAX: {len(reviews)}")

    shop["scraped_date"] = TODAY
    return items, reviews, shop


# ──────────────────────────────────────────────────────────────────────────────
# Cloudflare R2 upload
# ──────────────────────────────────────────────────────────────────────────────
def upload_image_to_r2(image_url: str, r2_path: str, s3: "boto3.client") -> str:
    """
    Download an image from a URL and upload it to Cloudflare R2.
    Returns the R2 path on success, empty string on failure.
    """
    if not image_url or not r2_path:
        return ""

    try:
        resp = SESSION.get(image_url, timeout=30, stream=True)
        resp.raise_for_status()

        content_type = resp.headers.get('Content-Type', 'image/jpeg')

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=r2_path,
            Body=resp.content,
            ContentType=content_type,
        )
        return r2_path
    except Exception as exc:
        log.debug(f"    Image upload failed {image_url} -> {r2_path}: {exc}")
        return ""


def upload_df_to_r2(df: "pd.DataFrame", s3: "boto3.client", key: str):
    """Serialize a DataFrame as UTF-8 CSV with BOM and put it in Cloudflare R2."""
    buf = StringIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    csv_bytes = buf.getvalue().encode("utf-8-sig")
    try:
        s3.put_object(
            Bucket      = S3_BUCKET,
            Key         = key,
            Body        = csv_bytes,
            ContentType = "text/csv; charset=utf-8",
        )
        log.info(f"✓  r2://{S3_BUCKET}/{key}  ({len(df)} rows)")
    except ClientError as exc:
        log.error(f"R2 upload failed for {key}: {exc}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Bleems.com shops → Cloudflare R2")
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="Specific category to scrape (e.g., 'Flowers', 'Confections'). If not provided, scrapes all categories."
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List available categories as JSON and exit (for GitHub Actions dynamic matrix)"
    )
    args = parser.parse_args()

    log.info(f"Run date   : {TODAY}")
    log.info(f"R2 bucket  : {S3_BUCKET}")
    log.info(f"R2 endpoint: {CF_R2_ENDPOINT_URL}")
    if args.category:
        log.info(f"Target category: {args.category}")

    # ── List categories mode ──────────────────────────────────────────────────
    if args.list_categories:
        all_shops = fetch_all_shops()
        if not all_shops:
            print(json.dumps([]))
            return

        categories = sorted(set(shop["type"] or "Other" for shop in all_shops))
        print(json.dumps(categories))
        return

    # ── Cloudflare R2 client ──────────────────────────────────────────────────
    # R2 requires: explicit credentials + custom endpoint_url + path-style addressing
    r2 = boto3.client(
        "s3",
        endpoint_url          = CF_R2_ENDPOINT_URL,
        aws_access_key_id     = CF_R2_ACCESS_KEY,
        aws_secret_access_key = CF_R2_SECRET_KEY,
        region_name           = "us-east-1",       # dummy value — R2 ignores region
        config=Config(
            signature_version = "s3v4",
            s3={"addressing_style": "path"},        # R2 requires path-style
        ),
    )

    # ── 0. Selector health check + DOM fingerprint comparison ─────────────────
    import selector_health as _health
    _health_passed, _fingerprints = _health.run_health_check(abort_on_failure=False)
    _health.compare_and_store_fingerprints(_fingerprints, r2, S3_BUCKET, S3_FOLDER)

    # ── 1. Fetch shop list ─────────────────────────────────────────────────────
    all_shops = fetch_all_shops()
    if not all_shops:
        log.error("No shops found – aborting.")
        return

    # ── 2. Group by type ───────────────────────────────────────────────────────
    by_type: dict[str, list[dict]] = {}
    for shop in all_shops:
        t = shop["type"] or "Other"
        by_type.setdefault(t, []).append(shop)

    log.info(f"Types detected: {sorted(by_type.keys())}")

    # ── 3. Filter by category if specified ────────────────────────────────────
    if args.category:
        if args.category in by_type:
            by_type = {args.category: by_type[args.category]}
            log.info(f"Processing only category: {args.category}")
        else:
            log.error(f"Category '{args.category}' not found. Available: {sorted(by_type.keys())}")
            return

    # ── 4. Process each type ──────────────────────────────────────────────────
    for shop_type, shops in sorted(by_type.items()):
        log.info(f"\n{'─'*60}")
        log.info(f"Processing type: {shop_type}  ({len(shops)} shops)")
        log.info(f"{'─'*60}")

        all_items:   list[dict] = []
        all_reviews: list[dict] = []
        enriched:    list[dict] = []

        for idx, shop in enumerate(shops, 1):
            log.info(f"  [{idx:>3}/{len(shops)}] {shop['name']}")

            if not shop.get("slug"):
                log.warning("    No slug – skipped")
                shop["r2_image_path"] = ""
                enriched.append(shop)
                continue

            items, reviews, updated_shop = fetch_shop_data(shop, r2)
            all_items.extend(items)
            all_reviews.extend(reviews)
            enriched.append(updated_shop)

            log.info(f"         items={len(items)}  reviews={len(reviews)}")
            time.sleep(REQUEST_DELAY)

        # R2 key prefix: bleems-data/year=2026/month=02/day=21/Flowers/
        prefix = f"{S3_FOLDER}/year={S3_YEAR}/month={S3_MONTH}/day={S3_DAY}/{shop_type}"

        # Deduplicate items by product_id
        items_before_dedup = len(all_items)
        seen_product_ids   = set()
        deduped_items      = []
        for item in all_items:
            pid = item.get("product_id")
            if pid and pid not in seen_product_ids:
                seen_product_ids.add(pid)
                deduped_items.append(item)
            elif not pid:
                deduped_items.append(item)

        if items_before_dedup > len(deduped_items):
            log.info(
                f"  Deduplicated items: {items_before_dedup} → {len(deduped_items)} "
                f"(removed {items_before_dedup - len(deduped_items)} duplicates)"
            )
        all_items = deduped_items

        shops_with_images = sum(1 for s in enriched   if s.get("r2_image_path"))
        items_with_images = sum(1 for i in all_items  if i.get("r2_image_path"))

        upload_df_to_r2(pd.DataFrame(enriched), r2, f"{prefix}/shops.csv")

        if all_items:
            upload_df_to_r2(pd.DataFrame(all_items), r2, f"{prefix}/items.csv")
        else:
            log.warning(f"  No items found for {shop_type}")

        if all_reviews:
            upload_df_to_r2(pd.DataFrame(all_reviews), r2, f"{prefix}/reviews.csv")
        else:
            log.warning(f"  No reviews found for {shop_type}")

        log.info(
            f"  Done {shop_type}: {len(enriched)} shops, {len(all_items)} items, "
            f"{len(all_reviews)} reviews | Images: {shops_with_images} shop logos, "
            f"{items_with_images} product images"
        )

    log.info("\nAll done!")


if __name__ == "__main__":
    main()
