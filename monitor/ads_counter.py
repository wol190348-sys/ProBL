"""
Unique ad counting for Bleems CSV-based scraper data.

Priority order:
  1. Unique product_id values from items.csv   → ads_source = "csv_ids"
  2. total_listings/total_ads from json-files/ → ads_source = "json_summary"
  3. Row count from items.csv                  → ads_source = "csv_rows"
  4. No data available                         → ads_source = "none"

Public API:
    count_scraper_ads(r2_client, bucket, prefixes) -> dict

Shared with the Pro1-Os monitor hub — do not add Bleems-specific logic below
the public function.  Bleems-specific details (CSV layout, id column name) are
handled transparently inside this module.
"""
from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd
from botocore.exceptions import ClientError

# ID column names accepted as "listing id" (checked in order)
_ID_COLS = [
    "product_id",
    "id",
    "listing_id",
    "listing id",
    "user_adv_id",
    "user adv id",
    "ad_id",
    "ad id",
]


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _download(client: Any, bucket: str, key: str) -> bytes | None:
    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError:
        return None


def _list_prefix(client: Any, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except ClientError:
        pass
    return keys


def _ids_and_rows_from_csv(raw: bytes) -> tuple[set[str], int]:
    """
    Parse a CSV from raw bytes and return (unique_id_set, total_row_count).

    Searches for the first recognised ID column.  Returns an empty set (not
    zero rows) when no ID column is found so the caller can fall back to the
    row-count path.
    """
    try:
        df = pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
    except Exception:
        return set(), 0

    total_rows = len(df)

    for col in _ID_COLS:
        if col in df.columns:
            ids = set(df[col].dropna().astype(str).str.strip())
            ids.discard("")
            return ids, total_rows

    return set(), total_rows


def _total_from_json_summary(client: Any, bucket: str, json_prefix: str) -> int | None:
    """
    Look for any .json file under *json_prefix* and return the first
    recognised count field (total_listings, total_ads, or listings_count).
    Returns None if nothing is found or parseable.
    """
    for key in sorted(_list_prefix(client, bucket, json_prefix)):
        if not key.endswith(".json"):
            continue
        raw = _download(client, bucket, key)
        if not raw:
            continue
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            continue
        for field in ("total_listings", "total_ads", "listings_count"):
            val = data.get(field)
            if val is not None:
                try:
                    return int(val)
                except (TypeError, ValueError):
                    pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def count_scraper_ads(
    r2_client: Any,
    bucket: str,
    prefixes: list[str],
) -> dict:
    """
    Count unique ads for one scraper across all given R2 prefixes.

    Each prefix corresponds to one date's partition folder and must end with
    a trailing slash, e.g.:
        "bleems-data/year=2026/month=06/day=24/Flowers/"

    Pass multiple prefixes when --days-lookback > 1; product IDs are unioned
    across all dates so the result reflects the total unique ad count for the
    monitored period.

    Returns:
        {
            "unique_ads": int,
            "total_rows": int,
            "ads_source": "csv_ids" | "json_summary" | "csv_rows" | "none",
        }
    """
    all_ids: set[str] = set()
    total_rows = 0
    has_rows = False

    for prefix in prefixes:
        raw = _download(r2_client, bucket, f"{prefix}items.csv")
        if raw is None:
            continue

        ids, rows = _ids_and_rows_from_csv(raw)
        total_rows += rows
        if rows:
            has_rows = True
        all_ids.update(ids)

    if all_ids:
        return {
            "unique_ads": len(all_ids),
            "total_rows": total_rows,
            "ads_source": "csv_ids",
        }

    # JSON summary fallback (for scrapers that upload json-files/)
    for prefix in prefixes:
        count = _total_from_json_summary(r2_client, bucket, f"{prefix}json-files/")
        if count is not None:
            return {
                "unique_ads": count,
                "total_rows": total_rows,
                "ads_source": "json_summary",
            }

    if has_rows:
        return {
            "unique_ads": total_rows,
            "total_rows": total_rows,
            "ads_source": "csv_rows",
        }

    return {"unique_ads": 0, "total_rows": 0, "ads_source": "none"}
