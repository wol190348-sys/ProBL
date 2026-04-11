"""
Test if the generated shop URLs are valid
"""
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Test a few generated URLs
test_urls = [
    "https://www.bleems.com/kw/shop/aroma-cake",
    "https://www.bleems.com/kw/shop/aunty-jujus",
    "https://www.bleems.com/kw/shop/api-life-honey",
]

for url in test_urls:
    print(f"\nTesting: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        print(f"  Status: {resp.status_code}")
        print(f"  Final URL: {resp.url}")
        if resp.status_code == 200:
            if "Page Not Found" in resp.text or "404" in resp.text:
                print("  ❌ Page exists but shows 404 content")
            else:
                print(f"  ✓ Page loaded successfully ({len(resp.text)} bytes)")
        else:
            print(f"  ❌ Failed")
    except Exception as e:
        print(f"  ❌ Error: {e}")
