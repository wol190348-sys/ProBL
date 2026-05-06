"""
selector_health.py — Cloudflare R2 edition
-------------------------------------------
Pre-run canary health check for every CSS selector used by CF/scraper.py.

Identical logic to the root-level selector_health.py; the only difference
is that compare_and_store_fingerprints() stores the fingerprint JSON in
Cloudflare R2 (passed in as the boto3 client configured with R2 credentials).

Usage (called automatically from CF/scraper.main()):
    import selector_health as _health
    _health_passed, _fingerprints = _health.run_health_check(abort_on_failure=False)
    _health.compare_and_store_fingerprints(_fingerprints, r2_client, bucket, folder)
"""

import difflib
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# selectors.json lives one level above CF/
_DIR            = Path(__file__).parent.parent
SELECTORS_FILE  = _DIR / "selectors.json"
BASE_URL        = "https://www.bleems.com"
COUNTRY         = "kw"
_HEADERS        = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_html(url: str) -> str | None:
    """Fetch a URL and return the decoded body, or None on error."""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=30)
        r.encoding = "utf-8"
        r.raise_for_status()
        return r.text
    except requests.RequestException as exc:
        log.warning(f"  [health] Could not fetch {url}: {exc}")
        return None


def _dom_classes(html: str) -> list[str]:
    """Return every unique CSS class name present anywhere on the page."""
    soup = BeautifulSoup(html, "html.parser")
    return sorted({c for el in soup.find_all(class_=True) for c in el.get("class", [])})


def compute_dom_fingerprint(html: str) -> str:
    """SHA-256 of the sorted, space-joined set of all CSS class names on the page."""
    classes = _dom_classes(html)
    return hashlib.sha256(" ".join(classes).encode()).hexdigest()


def _suggest(css: str, live_classes: list[str]) -> list[str]:
    """Return up to 3 close-match class names the selector might have been renamed to."""
    class_parts = re.findall(r"\.([\w-]+)", css)
    id_parts    = re.findall(r"#([\w-]+)",  css)
    if not class_parts and not id_parts:
        return []
    target = class_parts[0] if class_parts else id_parts[0]
    return difflib.get_close_matches(target, live_classes, n=3, cutoff=0.55)


def _test_group(
    group_name:   str,
    selectors:    dict,
    html:         str,
    live_classes: list[str],
    optional:     bool = False,
) -> dict[str, bool]:
    """
    For every key→css pair in *selectors*, run soup.select(css) against *html*.
    Returns a dict mapping "group.key" → True/False.
    """
    soup    = BeautifulSoup(html, "html.parser")
    results = {}

    for key, css in selectors.items():
        if key.startswith("_"):
            continue
        found = soup.select(css)
        ok    = len(found) > 0
        results[f"{group_name}.{key}"] = ok

        if not ok:
            suggestions = _suggest(css, live_classes)
            hint  = f"  → closest matches: {suggestions}" if suggestions else "  → no close match found on this page"
            level = log.debug if optional else log.warning
            level(
                f"  [health] {'OPTIONAL ' if optional else ''}BROKEN  [{group_name}.{key}]  '{css}'"
                + hint
            )

    return results


# ──────────────────────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────────────────────

def run_health_check(abort_on_failure: bool = False) -> tuple[bool, dict[str, str]]:
    """
    Fetch one sample page per tier and test every CSS selector.

    Page tiers:
      Tier 1  →  /kw/shops              (shop_list selectors)
      Tier 2  →  /kw/shop/<slug>        (product_card, shop_ratings, reviews)
      Tier 3  →  /kw/<cat>/<shop>/<id>  (product_detail)

    Returns (passed, fingerprints) where:
      passed       — True when all critical selectors matched
      fingerprints — SHA-256 hash of the CSS class vocabulary per tier
    Raises SystemExit(1) when abort_on_failure=True and any selector is broken.
    """
    with open(SELECTORS_FILE, encoding="utf-8") as f:
        selectors = json.load(f)

    log.info("─" * 60)
    log.info("Selector health check starting …")

    all_results:  dict[str, bool] = {}
    fingerprints: dict[str, str]  = {}
    fetch_failed = False

    # ── Tier 1: shop list page ────────────────────────────────────────────────
    shops_url  = f"{BASE_URL}/{COUNTRY}/shops"
    log.info(f"  [health] Tier 1 → {shops_url}")
    shops_html = _fetch_html(shops_url)

    sample_slug = None

    if shops_html:
        live_classes = _dom_classes(shops_html)
        fingerprints["shop_list"] = compute_dom_fingerprint(shops_html)
        r = _test_group("shop_list", selectors["shop_list"], shops_html, live_classes)
        all_results.update(r)

        soup       = BeautifulSoup(shops_html, "html.parser")
        shop_css   = selectors["shop_list"].get("shop_item", "")
        first_shop = soup.select_one(shop_css) if shop_css else None
        if first_shop:
            onclick = first_shop.get("onclick", "")
            m = re.search(r"onShopClicked\('([^']+)'", onclick)
            if m:
                sample_slug = m.group(1).split("/shop/")[-1].rstrip("/")
            else:
                href = first_shop.get("href", "")
                if "/shop/" in href:
                    sample_slug = href.split("/shop/")[-1].rstrip("/")
    else:
        fetch_failed = True
        log.warning("  [health] Could not fetch shop list — tier 1 skipped")

    time.sleep(1)

    # ── Tier 2: shop page ─────────────────────────────────────────────────────
    sample_product_url = None

    if sample_slug:
        shop_url  = f"{BASE_URL}/{COUNTRY}/shop/{sample_slug}"
        log.info(f"  [health] Tier 2 → {shop_url}")
        shop_html = _fetch_html(shop_url)

        if shop_html:
            live_classes = _dom_classes(shop_html)
            fingerprints["shop_page"] = compute_dom_fingerprint(shop_html)
            r = _test_group("product_card",          selectors["product_card"],          shop_html, live_classes)
            all_results.update(r)
            r = _test_group("product_card_fallback", selectors["product_card_fallback"], shop_html, live_classes, optional=True)
            all_results.update(r)
            r = _test_group("shop_ratings",          selectors["shop_ratings"],          shop_html, live_classes)
            all_results.update(r)
            r = _test_group("reviews",               selectors["reviews"],               shop_html, live_classes)
            all_results.update(r)

            card_soup  = BeautifulSoup(shop_html, "html.parser")
            head_css   = selectors["product_card"].get("head", "")
            first_head = card_soup.select_one(head_css) if head_css else None
            if first_head:
                target = first_head.get("data-content-target", "").lstrip("/")
                if target and "?source=ad" not in target:
                    sample_product_url = f"{BASE_URL}/{COUNTRY}/{target}"
        else:
            fetch_failed = True
            log.warning("  [health] Could not fetch shop page — tier 2 skipped")
    else:
        log.warning("  [health] No sample slug found — tier 2 skipped")

    time.sleep(1)

    # ── Tier 3: product detail page ───────────────────────────────────────────
    if sample_product_url:
        log.info(f"  [health] Tier 3 → {sample_product_url}")
        prod_html = _fetch_html(sample_product_url)

        if prod_html:
            live_classes = _dom_classes(prod_html)
            fingerprints["product_detail"] = compute_dom_fingerprint(prod_html)
            r = _test_group("product_detail", selectors["product_detail"], prod_html, live_classes)
            all_results.update(r)
        else:
            fetch_failed = True
            log.warning("  [health] Could not fetch product page — tier 3 skipped")
    else:
        log.warning("  [health] No sample product URL found — tier 3 skipped")

    # ── Summary ───────────────────────────────────────────────────────────────
    critical_results = {
        k: v for k, v in all_results.items()
        if not k.startswith("product_card_fallback.")
    }

    broken   = [k for k, v in critical_results.items() if not v]
    ok_count = sum(v for v in critical_results.values())
    total    = len(critical_results)

    log.info(f"  [health] {ok_count}/{total} critical selectors OK")

    if broken:
        log.warning(f"  [health] BROKEN selectors ({len(broken)}): {broken}")
    else:
        log.info("  [health] All critical selectors healthy ✓")

    log.info("─" * 60)

    passed = not broken and not fetch_failed

    if not passed and abort_on_failure:
        raise SystemExit(1)

    return passed, fingerprints


# ──────────────────────────────────────────────────────────────────────────────
# DOM fingerprint comparison  (stored in Cloudflare R2)
# ──────────────────────────────────────────────────────────────────────────────

def compare_and_store_fingerprints(
    fingerprints: dict[str, str],
    r2_client,
    bucket: str,
    r2_folder: str,
) -> None:
    """
    Compare today's DOM fingerprints against the last stored ones in Cloudflare R2.
    Logs a WARNING for any tier whose CSS class vocabulary has changed since
    the last run — an early signal that a site redesign may break selectors.

    R2 key  : <r2_folder>/dom_fingerprints.json
    Format  : {"shop_list": {"hash": "...", "updated": "YYYY-MM-DD"}, ...}

    NOTE: Uses the same boto3 put_object / get_object API as AWS S3 — R2 is
    fully S3-compatible.  The client must be initialised with endpoint_url,
    signature_version='s3v4', and path-style addressing (see CF/scraper.py).
    """
    if not bucket:
        log.debug("  [fingerprint] R2 bucket not configured — skipping")
        return

    key   = f"{r2_folder}/dom_fingerprints.json"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── Load previous fingerprints from R2 ────────────────────────────────────
    previous: dict = {}
    try:
        resp     = r2_client.get_object(Bucket=bucket, Key=key)
        previous = json.loads(resp["Body"].read().decode("utf-8"))
        log.info(f"  [fingerprint] Loaded previous fingerprints from r2://{bucket}/{key}")
    except Exception:
        log.info("  [fingerprint] No previous fingerprints found — first run or R2 unavailable")

    # ── Compare tier by tier ──────────────────────────────────────────────────
    changed: list[str] = []
    for tier, new_hash in fingerprints.items():
        old_entry = previous.get(tier, {})
        old_hash  = old_entry.get("hash", "")
        if old_hash and old_hash != new_hash:
            changed.append(tier)
            log.warning(
                f"  [fingerprint] DOM CHANGED  tier='{tier}'  "
                f"old={old_hash[:12]}…  new={new_hash[:12]}…  "
                f"(last seen: {old_entry.get('updated', 'unknown')})"
            )

    if not changed:
        log.info("  [fingerprint] DOM fingerprints unchanged ✓")
    else:
        log.warning(
            f"  [fingerprint] {len(changed)} tier(s) changed: {changed}  "
            "— review selectors.json if scraping starts failing"
        )

    # ── Write new fingerprints back to R2 ─────────────────────────────────────
    new_data = {
        tier: {"hash": h, "updated": today}
        for tier, h in fingerprints.items()
    }
    try:
        r2_client.put_object(
            Bucket      = bucket,
            Key         = key,
            Body        = json.dumps(new_data, indent=2).encode("utf-8"),
            ContentType = "application/json",
        )
        log.info(f"  [fingerprint] Saved fingerprints → r2://{bucket}/{key}")
    except Exception as exc:
        log.warning(f"  [fingerprint] Could not save fingerprints to R2: {exc}")
