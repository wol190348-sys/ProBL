"""
HTTP request throughput and error metrics for monitor hub dashboard rollups.

Public API:
    count_scraper_request_metrics(r2_client, bucket, r2_base, partition_dt, *, date_first=False) -> dict
    aggregate_site_request_metrics(all_results) -> dict
    build_run_error_summary(all_results, alerts=None) -> dict

Shared with the Pro1-Os monitor hub — do not add site-specific logic below
the public functions.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from botocore.exceptions import ClientError

_TOTAL_KEYS = ("requests_total", "total_http_requests", "scrape_do_requests", "request_count")
_FAILED_KEYS = ("requests_failed", "failed_requests", "http_errors", "errors_count")
_DURATION_KEYS = ("duration_sec", "elapsed_seconds", "scrape_duration_sec")
_RPM_KEYS = ("requests_per_min", "req_per_min", "avg_requests_per_min")


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


def _date_parts(partition_dt: date) -> tuple[str, str, str]:
    return (
        str(partition_dt.year),
        f"{partition_dt.month:02d}",
        f"{partition_dt.day:02d}",
    )


def _json_files_prefix(r2_base: str, partition_dt: date, *, date_first: bool) -> str:
    year, month, day = _date_parts(partition_dt)
    base = r2_base.strip("/")
    if date_first:
        parts = base.split("/", 1)
        if len(parts) == 2:
            site_prefix, category = parts
            return f"{site_prefix}/year={year}/month={month}/day={day}/{category}/json-files/"
    return f"{base}/year={year}/month={month}/day={day}/json-files/"


def _pick_int(block: dict, keys: tuple[str, ...]) -> int | None:
    for key in keys:
        val = block.get(key)
        if val is None:
            continue
        try:
            return int(val)
        except (TypeError, ValueError):
            continue
    return None


def _pick_float(block: dict, keys: tuple[str, ...]) -> float | None:
    for key in keys:
        val = block.get(key)
        if val is None:
            continue
        try:
            return float(val)
        except (TypeError, ValueError):
            continue
    return None


def _extract_metrics_block(data: dict) -> dict:
    for source in (data.get("request_metrics"), data.get("stats"), data):
        if not isinstance(source, dict):
            continue
        block: dict[str, Any] = {}
        total = _pick_int(source, _TOTAL_KEYS)
        failed = _pick_int(source, _FAILED_KEYS)
        duration = _pick_int(source, _DURATION_KEYS)
        rpm = _pick_float(source, _RPM_KEYS)
        failed_items = source.get("failed_items")
        if total is not None:
            block["requests_total"] = total
        if failed is not None:
            block["requests_failed"] = failed
        if duration is not None:
            block["duration_sec"] = duration
        if rpm is not None:
            block["requests_per_min"] = rpm
        if isinstance(failed_items, list) and failed_items:
            block["failed_items"] = failed_items
        if block:
            return block
    return {}


def _compute_rates(
    requests_total: int | None,
    requests_failed: int | None,
    duration_sec: int | None,
    requests_per_min: float | None,
) -> tuple[float | None, float | None]:
    error_rate_pct = None
    if requests_total and requests_failed is not None:
        error_rate_pct = round(requests_failed / requests_total * 100, 2)

    if requests_per_min is None and requests_total and duration_sec and duration_sec > 0:
        requests_per_min = round(requests_total / (duration_sec / 60), 2)

    return error_rate_pct, requests_per_min


def _format_failed_items(items: list[dict]) -> str | None:
    if not items:
        return None
    parts: list[str] = []
    for item in items:
        name = item.get("name") or item.get("slug") or "unknown"
        errors = item.get("errors", 1)
        detail = item.get("detail") or ""
        suffix = f" ({detail})" if detail else ""
        parts.append(f"{name}: {errors} error(s){suffix}")
    return "; ".join(parts)


def _metrics_from_json(client: Any, bucket: str, json_prefix: str) -> dict:
    best: dict[str, Any] = {}
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
        block = _extract_metrics_block(data)
        if not block:
            continue
        if not best or block.get("requests_total", 0) >= best.get("requests_total", 0):
            best = block
    return best


def count_scraper_request_metrics(
    r2_client: Any,
    bucket: str,
    r2_base: str,
    partition_dt: date,
    *,
    date_first: bool = False,
) -> dict:
    """
    Read request_metrics from json-files/ summaries for one scraper partition.

    Returns:
        {
            "requests_total": int | None,
            "requests_failed": int | None,
            "error_rate_pct": float | None,
            "requests_per_min": float | None,
            "duration_sec": int | None,
            "metrics_source": "json_summary" | "none",
            "failed_items_summary": str | None,
        }
    """
    json_prefix = _json_files_prefix(r2_base, partition_dt, date_first=date_first)
    block = _metrics_from_json(r2_client, bucket, json_prefix)
    if not block:
        return {"metrics_source": "none"}

    requests_total = block.get("requests_total")
    requests_failed = block.get("requests_failed")
    duration_sec = block.get("duration_sec")
    requests_per_min = block.get("requests_per_min")
    error_rate_pct, requests_per_min = _compute_rates(
        requests_total, requests_failed, duration_sec, requests_per_min
    )

    result: dict[str, Any] = {"metrics_source": "json_summary"}
    if requests_total is not None:
        result["requests_total"] = requests_total
    if requests_failed is not None:
        result["requests_failed"] = requests_failed
    if error_rate_pct is not None:
        result["error_rate_pct"] = error_rate_pct
    if requests_per_min is not None:
        result["requests_per_min"] = requests_per_min
    if duration_sec is not None:
        result["duration_sec"] = duration_sec

    failed_items = block.get("failed_items")
    if isinstance(failed_items, list):
        summary = _format_failed_items(failed_items)
        if summary:
            result["failed_items_summary"] = summary

    return result


def aggregate_site_request_metrics(all_results: list[dict]) -> dict:
    """Roll up per-scraper HTTP metrics into site-level totals."""
    total_requests = 0
    total_failed = 0
    total_duration = 0
    has_requests = False
    has_failed = False
    has_duration = False

    for result in all_results:
        req_total = result.get("requests_total")
        req_failed = result.get("requests_failed")
        duration = result.get("duration_sec")

        if req_total is not None:
            total_requests += int(req_total)
            has_requests = True
        if req_failed is not None:
            total_failed += int(req_failed)
            has_failed = True
        if duration is not None:
            total_duration += int(duration)
            has_duration = True

    if not has_requests:
        return {}

    site: dict[str, Any] = {"requests_total": total_requests}
    if has_failed:
        site["requests_failed"] = total_failed
    error_rate_pct, requests_per_min = _compute_rates(
        total_requests,
        total_failed if has_failed else None,
        total_duration if has_duration else None,
        None,
    )
    if error_rate_pct is not None:
        site["error_rate_pct"] = error_rate_pct
    if requests_per_min is not None:
        site["requests_per_min"] = requests_per_min
    return site


def _scraper_failure_reason(scraper_result: dict) -> str:
    for file_result in scraper_result.get("files", []):
        for chk in file_result.get("checks", []):
            if not chk.get("passed"):
                return chk.get("detail") or chk.get("check") or "validation failed"
    if scraper_result.get("all_passed") is False:
        return "validation failed"
    return "unknown"


def build_run_error_summary(
    all_results: list[dict],
    alerts: list[dict] | None = None,
) -> dict:
    """Build site-level error_summary for report.json."""
    scrapers_total = len(all_results)
    failed_scrapers: list[dict[str, Any]] = []

    for result in all_results:
        if result.get("all_passed", True):
            continue
        failed_scrapers.append({
            "scraper": result.get("scraper", "?"),
            "reason": _scraper_failure_reason(result),
            "requests_failed": result.get("requests_failed"),
        })

    for alert in alerts or []:
        if not isinstance(alert, dict):
            continue
        entry = {
            "scraper": alert.get("scraper", "?"),
            "reason": alert.get("reason") or alert.get("detail") or "alert",
            "requests_failed": alert.get("requests_failed"),
        }
        if entry not in failed_scrapers:
            failed_scrapers.append(entry)

    scrapers_failed = len(failed_scrapers)
    scrapers_passed = max(scrapers_total - scrapers_failed, 0)
    validation_fail_rate_pct = (
        round(scrapers_failed / scrapers_total * 100, 2) if scrapers_total else 0.0
    )

    summary: dict[str, Any] = {
        "scrapers_total": scrapers_total,
        "scrapers_failed": scrapers_failed,
        "scrapers_passed": scrapers_passed,
        "validation_fail_rate_pct": validation_fail_rate_pct,
        "failed_scrapers": failed_scrapers,
    }

    http = aggregate_site_request_metrics(all_results)
    if http:
        summary["http"] = http
    return summary
