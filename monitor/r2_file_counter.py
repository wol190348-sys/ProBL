"""
Total R2 object inventory for monitor hub dashboard rollups.

Public API:
    count_scraper_r2_files(client, bucket, r2_base) -> int
    count_site_r2_files(client, bucket, r2_prefix) -> int

Shared with the Pro1-Os monitor hub — do not add site-specific logic here.
"""
from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError


def _normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip("/")
    return f"{prefix}/" if prefix else ""


def _count_objects_under_prefix(client: Any, bucket: str, prefix: str) -> int:
    """Paginated list_objects_v2 count; excludes folder marker keys."""
    prefix = _normalize_prefix(prefix)
    count = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("/"):
                    count += 1
    except ClientError:
        pass
    return count


def count_scraper_r2_files(client: Any, bucket: str, r2_base: str) -> int:
    """Count all objects under a scraper's r2_base prefix (historical inventory)."""
    return _count_objects_under_prefix(client, bucket, r2_base)


def count_site_r2_files(client: Any, bucket: str, r2_prefix: str) -> int:
    """Count all objects under the site r2_prefix (includes monitor/ artifacts)."""
    return _count_objects_under_prefix(client, bucket, r2_prefix)
