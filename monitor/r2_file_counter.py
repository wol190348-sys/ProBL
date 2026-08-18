"""
Total R2 object inventory for monitor hub dashboard rollups.

Public API:
    count_scraper_r2_files(client, bucket, r2_base) -> int
    count_site_r2_files(client, bucket, r2_prefix) -> int
    get_scraper_r2_inventory(client, bucket, r2_base) -> dict
    get_site_r2_inventory(client, bucket, r2_prefix) -> dict
    get_partition_r2_inventory(client, bucket, partition_prefix) -> dict
    count_r2_inventory_by_type(client, bucket, prefix) -> dict
    count_daily_r2_inventory_by_type(client, bucket, r2_base, partition_dt) -> dict
    count_scraper_r2_inventory_by_type(client, bucket, r2_base) -> dict
    count_site_r2_inventory_by_type(client, bucket, r2_prefix) -> dict

Shared with the Pro1-Os monitor hub — do not add site-specific logic here.
"""
from __future__ import annotations

import os
import re
from datetime import date
from typing import Any

from botocore.exceptions import ClientError

_TYPE_CATEGORIES: tuple[str, ...] = ("images", "json", "excel", "csv", "parquet", "other")
_IMAGES_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff", ".svg"}
_JSON_EXTS = {".json"}
_EXCEL_EXTS = {".xlsx", ".xls", ".xlsm"}
_CSV_EXTS = {".csv"}
_PARQUET_EXTS = {".parquet"}
_MONTH_DAY_RE = re.compile(r"month=(\d+?)/day=(\d+?)/")
_DAILY_PARTITION_RE = re.compile(r"/year=\d+/month=\d+/day=\d+/")


def _file_type_for_key(key: str) -> str:
    ext = os.path.splitext(key.lower())[1]
    if ext in _IMAGES_EXTS:
        return "images"
    if ext in _JSON_EXTS:
        return "json"
    if ext in _EXCEL_EXTS:
        return "excel"
    if ext in _CSV_EXTS:
        return "csv"
    if ext in _PARQUET_EXTS:
        return "parquet"
    return "other"


def _empty_type_inventory() -> dict[str, Any]:
    return {
        "objects": 0,
        "size_bytes": 0,
        "by_type_objects": {cat: 0 for cat in _TYPE_CATEGORIES},
        "by_type_bytes": {cat: 0 for cat in _TYPE_CATEGORIES},
    }


def _normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip("/")
    return f"{prefix}/" if prefix else ""


def _inventory_under_prefix(client: Any, bucket: str, prefix: str) -> tuple[int, int]:
    """Paginated list_objects_v2 inventory; excludes folder marker keys."""
    prefix = _normalize_prefix(prefix)
    count = 0
    total_bytes = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/"):
                    count += 1
                    total_bytes += int(obj.get("Size", 0) or 0)
    except ClientError:
        pass
    return count, total_bytes


def _inventory_dict(client: Any, bucket: str, prefix: str) -> dict[str, int]:
    file_count, total_bytes = _inventory_under_prefix(client, bucket, prefix)
    return {
        "file_count": file_count,
        "total_size_bytes": total_bytes,
    }


def _accumulate_object(
    inventory: dict[str, Any],
    key: str,
    obj_size: int,
) -> None:
    cat = _file_type_for_key(key)
    inventory["objects"] += 1
    inventory["size_bytes"] += obj_size
    inventory["by_type_objects"][cat] += 1
    inventory["by_type_bytes"][cat] += obj_size


def _inventory_under_prefix_by_type(client: Any, bucket: str, prefix: str) -> dict[str, Any]:
    """Paginated list_objects_v2 inventory split by file extension category."""
    prefix = _normalize_prefix(prefix)
    inventory = _empty_type_inventory()
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                _accumulate_object(inventory, key, int(obj.get("Size", 0) or 0))
    except ClientError:
        pass
    return inventory


def _date_parts(partition_dt: date) -> tuple[str, str, str]:
    return (
        str(partition_dt.year),
        f"{partition_dt.month:02d}",
        f"{partition_dt.day:02d}",
    )


def _daily_prefix_from_r2_base(r2_base: str, partition_dt: date) -> str:
    """Build a daily partition prefix from scraper base or accept a full partition path."""
    base = r2_base.strip("/")
    if _DAILY_PARTITION_RE.search(f"/{base}/"):
        return _normalize_prefix(base)

    year, month, day = _date_parts(partition_dt)
    parts = base.split("/", 1)
    if len(parts) == 2:
        site_prefix, category = parts
        return f"{site_prefix}/year={year}/month={month}/day={day}/{category}/"
    return f"{base}/year={year}/month={month}/day={day}/"


def _daily_prefix_variants(partition_prefix: str) -> set[str]:
    """
    Produce padded and unpadded month/day variants when they differ.

    Avoids double-counting when legacy unpadded folders coexist.
    """
    partition_prefix = _normalize_prefix(partition_prefix)
    candidates = {partition_prefix}

    match = _MONTH_DAY_RE.search(partition_prefix)
    if not match:
        return candidates

    month_raw = match.group(1)
    day_raw = match.group(2)
    month_unpadded = str(int(month_raw))
    day_unpadded = str(int(day_raw))

    if month_unpadded == month_raw and day_unpadded == day_raw:
        return candidates

    alt = re.sub(
        rf"month={re.escape(month_raw)}/day={re.escape(day_raw)}/",
        f"month={month_unpadded}/day={day_unpadded}/",
        partition_prefix,
        count=1,
    )
    candidates.add(_normalize_prefix(alt))
    return candidates


def count_r2_inventory_by_type(client: Any, bucket: str, prefix: str) -> dict[str, Any]:
    """Count all objects under `prefix`, split into file-type buckets."""
    return _inventory_under_prefix_by_type(client, bucket, prefix)


def count_daily_r2_inventory_by_type(
    client: Any,
    bucket: str,
    r2_base: str,
    partition_dt: date,
) -> dict[str, Any]:
    """Daily inventory split by file type, deduping padded vs unpadded month/day prefixes."""
    partition_prefix = _daily_prefix_from_r2_base(r2_base, partition_dt)
    prefixes = _daily_prefix_variants(partition_prefix)

    inventory = _empty_type_inventory()
    seen: set[str] = set()

    try:
        paginator = client.get_paginator("list_objects_v2")
        for prefix in prefixes:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key in seen or key.endswith("/"):
                        continue
                    seen.add(key)
                    _accumulate_object(inventory, key, int(obj.get("Size", 0) or 0))
    except ClientError:
        pass

    return inventory


def count_scraper_r2_inventory_by_type(client: Any, bucket: str, r2_base: str) -> dict[str, Any]:
    """Historical inventory for a scraper prefix, split by file type."""
    return count_r2_inventory_by_type(client, bucket, r2_base)


def count_site_r2_inventory_by_type(client: Any, bucket: str, r2_prefix: str) -> dict[str, Any]:
    """Historical inventory for a site prefix, split by file type."""
    return count_r2_inventory_by_type(client, bucket, r2_prefix)


def count_scraper_r2_files(client: Any, bucket: str, r2_base: str) -> int:
    """Count all objects under a scraper's r2_base prefix (historical inventory)."""
    file_count, _ = _inventory_under_prefix(client, bucket, r2_base)
    return file_count


def count_site_r2_files(client: Any, bucket: str, r2_prefix: str) -> int:
    """Count all objects under the site r2_prefix (includes monitor/ artifacts)."""
    file_count, _ = _inventory_under_prefix(client, bucket, r2_prefix)
    return file_count


def get_scraper_r2_inventory(client: Any, bucket: str, r2_base: str) -> dict[str, int]:
    """Get object count and total size for a scraper R2 prefix."""
    return _inventory_dict(client, bucket, r2_base)


def get_site_r2_inventory(client: Any, bucket: str, r2_prefix: str) -> dict[str, int]:
    """Get object count and total size for a site R2 prefix."""
    return _inventory_dict(client, bucket, r2_prefix)


def get_partition_r2_inventory(client: Any, bucket: str, partition_prefix: str) -> dict[str, int]:
    """Get object count and total size for a single date partition prefix."""
    return _inventory_dict(client, bucket, partition_prefix)
