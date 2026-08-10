"""
Total R2 object inventory for monitor hub dashboard rollups.

Public API:
    count_scraper_r2_files(client, bucket, r2_base) -> int
    count_site_r2_files(client, bucket, r2_prefix) -> int
    get_scraper_r2_inventory(client, bucket, r2_base) -> dict
    get_site_r2_inventory(client, bucket, r2_prefix) -> dict
    get_partition_r2_inventory(client, bucket, partition_prefix) -> dict

Shared with the Pro1-Os monitor hub — do not add site-specific logic here.
"""
from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError


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
