"""
Debug script to inspect shop links from bleems.com
Run this to see what href values are actually in the page
"""

import re
import requests
from bs4 import BeautifulSoup

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

url = "https://www.bleems.com/kw/shops"
print(f"Fetching: {url}\n")

resp = requests.get(url, headers=HEADERS, timeout=30)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "html.parser")

shops = soup.select("a.brand-a-z-list-item")
print(f"Found {len(shops)} shop links\n")

if shops:
    print("First 10 shop links (with NEW slug generation):")
    print("=" * 80)
    for i, shop in enumerate(shops[:10], 1):
        href = shop.get("href", "")
        name_div = shop.select_one(".brand-a-z-item-name")
        name = name_div.text.strip() if name_div else shop.get("data-name", "")
        data_name = shop.get("data-name", "")
        
        # NEW slug generation logic
        slug = ""
        if "/shop/" in href and "javascript:" not in href:
            slug = href.split("/shop/")[-1].rstrip("/")
        elif href.startswith("/") and "javascript:" not in href:
            parts = [p for p in href.split("/") if p]
            if len(parts) >= 2:
                slug = parts[-1]
        else:
            # Use data-name to construct slug
            if data_name:
                slug = re.sub(r'[^\w\s-]', '', data_name.lower())
                slug = re.sub(r'[-\s]+', '-', slug).strip('-')
        
        shop_url = f"https://www.bleems.com/kw/shop/{slug}" if slug else ""
        
        print(f"{i}. {name}")
        print(f"   data-name: {data_name}")
        print(f"   NEW slug: '{slug}'")
        print(f"   NEW URL: {shop_url}")
        print()
else:
    print("WARNING: No shop links found!")
    print("\nSearching for alternative selectors:")
    print(f"  .brand-a-z-list-item: {len(soup.select('.brand-a-z-list-item'))}")
    print(f"  a[data-name]: {len(soup.select('a[data-name]'))}")
    print(f"  .shop-link: {len(soup.select('.shop-link'))}")
    shop_hrefs = soup.select('a[href*="shop"]')
    print(f"  a[href*='shop']: {len(shop_hrefs)}")
    
    print("\n\nFirst 500 chars of HTML:")
    print(resp.text[:500])
