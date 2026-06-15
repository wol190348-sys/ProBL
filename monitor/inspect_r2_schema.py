#!/usr/bin/env python3
"""
Validate Bleems CSV outputs in Cloudflare R2 against csv_schema in websites-config.yml.

Config is read from R2: bleems-data/monitor/websites-config.yml
Reports and stats are also written to R2 only — never to the local repo.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import pandas as pd
import yaml
from botocore.exceptions import ClientError

R2_PREFIX = "bleems-data"
CONFIG_R2_KEY = f"{R2_PREFIX}/monitor/websites-config.yml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate R2 CSV outputs against csv_schema")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    parser.add_argument("--date", default=yesterday.isoformat(), help="Target date YYYY-MM-DD (UTC)")
    parser.add_argument("--days-lookback", type=int, default=1, help="Number of days to check")
    parser.add_argument("--update-stats", action="store_true", help="Merge observations into monitor_stats.yml in R2")
    parser.add_argument("--quality", action="store_true", help="Run deep data-quality checks")
    parser.add_argument("--fail-on-error", action="store_true", help="Exit 1 if any check failed")
    parser.add_argument(
        "--config-local",
        metavar="PATH",
        help="Load config from a local file instead of R2 (for development)",
    )
    return parser.parse_args()


def resolve_bucket(config: dict | None = None) -> str:
    if config and config.get("meta", {}).get("r2_bucket"):
        return config["meta"]["r2_bucket"]
    bucket = os.environ.get("CF_R2_BUCKET_NAME")
    if not bucket:
        sys.exit("CF_R2_BUCKET_NAME env var is required to load config from R2")
    return bucket


def load_config(client: Any, bucket: str, local_path: str | None = None) -> dict:
    if local_path:
        with open(local_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    raw = download_bytes(client, bucket, CONFIG_R2_KEY)
    if raw is None:
        sys.exit(
            f"Could not load config from s3://{bucket}/{CONFIG_R2_KEY} — "
            "upload websites-config.yml to that path first"
        )
    try:
        return yaml.safe_load(raw.decode("utf-8"))
    except yaml.YAMLError as exc:
        sys.exit(f"Invalid YAML in s3://{bucket}/{CONFIG_R2_KEY}: {exc}")


def build_r2_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CF_R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )


def date_parts(d: date) -> tuple[str, str, str]:
    return d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")


def partition_prefix(category_folder: str, d: date) -> str:
    year, month, day = date_parts(d)
    return f"{R2_PREFIX}/year={year}/month={month}/day={day}/{category_folder}/"


def list_objects(client: Any, bucket: str, prefix: str) -> list[dict]:
    keys: list[dict] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj)
    return keys


def download_bytes(client: Any, bucket: str, key: str) -> bytes | None:
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except ClientError:
        return None


def check(name: str, passed: bool, detail: str, severity: str = "critical") -> dict:
    return {"check": name, "passed": passed, "detail": detail, "severity": severity}


def validate_columns(headers: list[str], required: list[str]) -> tuple[bool, str]:
    missing = [c for c in required if c not in headers]
    if missing:
        return False, f"Missing columns: {missing}"
    return True, "All required columns present"


def run_quality_checks(df: pd.DataFrame, file_name: str) -> list[dict]:
    results: list[dict] = []
    if file_name == "items.csv" and "product_id" in df.columns:
        null_pct = df["product_id"].isna().mean() * 100
        dupes = int(df["product_id"].dropna().duplicated().sum())
        results.append(check(
            "null_product_id_pct",
            null_pct < 5,
            f"{null_pct:.1f}% null product_id",
            "high" if null_pct >= 5 else "medium",
        ))
        results.append(check(
            "duplicate_product_id",
            dupes == 0,
            f"{dupes} duplicate product_id values",
            "high" if dupes else "medium",
        ))
        if "product_name" in df.columns:
            null_name = df["product_name"].isna().mean() * 100
            results.append(check(
                "null_product_name_pct",
                null_name < 10,
                f"{null_name:.1f}% null product_name",
                "medium",
            ))
        if "price" in df.columns:
            null_price = df["price"].replace("", pd.NA).isna().mean() * 100
            results.append(check(
                "null_price_pct",
                null_price < 20,
                f"{null_price:.1f}% null/empty price",
                "medium",
            ))
    if file_name == "shops.csv" and "slug" in df.columns:
        null_slug = df["slug"].replace("", pd.NA).isna().mean() * 100
        results.append(check(
            "null_slug_pct",
            null_slug < 5,
            f"{null_slug:.1f}% null/empty slug",
            "high" if null_slug >= 5 else "medium",
        ))
    if file_name == "reviews.csv" and "star_rating" in df.columns and len(df):
        null_rating = df["star_rating"].replace("", pd.NA).isna().mean() * 100
        results.append(check(
            "null_star_rating_pct",
            null_rating < 10,
            f"{null_rating:.1f}% null/empty star_rating",
            "medium",
        ))
    return results


def validate_file(
    client: Any,
    bucket: str,
    key: str,
    file_spec: dict,
    quality: bool,
) -> dict:
    file_name = file_spec["name"]
    checks: list[dict] = []
    row_count = 0
    columns: list[str] = []

    raw = download_bytes(client, bucket, key)
    if raw is None:
        checks.append(check("file_readable", False, f"Could not download {key}"))
        return {"file": file_name, "key": key, "checks": checks, "row_count": 0, "columns": []}

    checks.append(check("file_readable", True, "Downloaded OK"))

    size_kb = len(raw) / 1024
    min_kb = file_spec.get("min_file_size_kb", 0)
    checks.append(check(
        "min_file_size_kb",
        size_kb >= min_kb,
        f"{size_kb:.1f} KB (min {min_kb} KB)",
        "high" if size_kb < min_kb else "medium",
    ))

    try:
        df = pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
    except Exception as exc:
        checks.append(check("csv_parse", False, str(exc)))
        return {"file": file_name, "key": key, "checks": checks, "row_count": 0, "columns": []}

    checks.append(check("csv_parse", True, "Parsed OK"))
    columns = list(df.columns)
    row_count = len(df)

    ok, detail = validate_columns(columns, file_spec.get("required_columns", []))
    checks.append(check("required_columns", ok, detail))

    lo, hi = file_spec.get("row_count_range", [0, 999999])
    in_range = lo <= row_count <= hi
    checks.append(check(
        "row_count_range",
        in_range,
        f"{row_count} rows (expected {lo}–{hi})",
        "high" if not in_range else "medium",
    ))

    if quality:
        checks.extend(run_quality_checks(df, file_name))

    return {
        "file": file_name,
        "key": key,
        "checks": checks,
        "row_count": row_count,
        "columns": columns,
        "size_kb": round(size_kb, 2),
    }


def load_existing_stats(client: Any, bucket: str) -> dict:
    key = f"{R2_PREFIX}/monitor/monitor_stats.yml"
    raw = download_bytes(client, bucket, key)
    if not raw:
        return {}
    try:
        return yaml.safe_load(raw.decode("utf-8")) or {}
    except yaml.YAMLError:
        return {}


def merge_stats(existing: dict, report: dict) -> dict:
    stats = existing.copy()
    stats.setdefault("scrapers", {})
    for scraper_result in report.get("scrapers", []):
        name = scraper_result["scraper"]
        entry = stats["scrapers"].setdefault(name, {"files": {}})
        for file_result in scraper_result.get("files", []):
            fname = file_result["file"]
            fstats = entry["files"].setdefault(fname, {
                "row_count_min": file_result.get("row_count", 0),
                "row_count_max": file_result.get("row_count", 0),
                "size_kb_min": file_result.get("size_kb", 0),
                "size_kb_max": file_result.get("size_kb", 0),
                "columns_seen": [],
            })
            rc = file_result.get("row_count", 0)
            sk = file_result.get("size_kb", 0)
            fstats["row_count_min"] = min(fstats.get("row_count_min", rc), rc)
            fstats["row_count_max"] = max(fstats.get("row_count_max", rc), rc)
            fstats["size_kb_min"] = min(fstats.get("size_kb_min", sk), sk)
            fstats["size_kb_max"] = max(fstats.get("size_kb_max", sk), sk)
            union = set(fstats.get("columns_seen", [])) | set(file_result.get("columns", []))
            fstats["columns_seen"] = sorted(union)
    stats["last_updated"] = datetime.now(timezone.utc).isoformat()
    return stats


def upload_json(client: Any, bucket: str, key: str, payload: dict) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def upload_yaml(client: Any, bucket: str, key: str, payload: dict) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=yaml.dump(payload, default_flow_style=False, allow_unicode=True).encode("utf-8"),
        ContentType="text/yaml",
    )


def write_step_summary(report: dict) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    lines = ["## R2 CSV Monitor\n", "| Scraper | Files | Passed | Total | Status |", "|---|---:|---:|---:|---|"]
    for s in report["scrapers"]:
        status = "✅" if s["all_passed"] else "❌"
        lines.append(
            f"| {s['scraper']} | {s['files_found']} | {s['checks_passed']} | {s['checks_total']} | {status} |"
        )
    with open(summary_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def print_summary(report: dict) -> None:
    print(f"\n{'Scraper':<16} {'Files':>5} {'Pass':>6} {'Total':>6}  Status")
    print("-" * 50)
    for s in report["scrapers"]:
        status = "OK" if s["all_passed"] else "FAIL"
        print(
            f"{s['scraper']:<16} {s['files_found']:>5} "
            f"{s['checks_passed']:>6} {s['checks_total']:>6}  {status}"
        )


def main() -> int:
    args = parse_args()
    client = build_r2_client()
    bucket = resolve_bucket()
    config = load_config(client, bucket, local_path=args.config_local)
    bucket = resolve_bucket(config)
    print(f"Config loaded from {'local file' if args.config_local else f's3://{bucket}/{CONFIG_R2_KEY}'}")

    schema_list = config.get("csv_schema") or config.get("excel_schema") or []
    start = date.fromisoformat(args.date)
    dates = [start - timedelta(days=i) for i in range(args.days_lookback)]

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dates_checked": [d.isoformat() for d in dates],
        "scrapers": [],
    }
    any_failed = False

    for schema in schema_list:
        scraper_name = schema["scraper"]
        category = schema.get("category_folder", scraper_name)
        scraper_result: dict[str, Any] = {
            "scraper": scraper_name,
            "files_found": 0,
            "files": [],
            "checks_passed": 0,
            "checks_total": 0,
            "all_passed": True,
        }

        for d in dates:
            prefix = partition_prefix(category, d)
            objects = {os.path.basename(o["Key"]): o for o in list_objects(client, bucket, prefix)}

            for file_spec in schema.get("files") or schema.get("sheets") or []:
                fname = file_spec["name"]
                optional = file_spec.get("optional", False)
                obj = objects.get(fname)

                if obj is None:
                    if optional:
                        continue
                    scraper_result["files"].append({
                        "file": fname,
                        "key": f"{prefix}{fname}",
                        "checks": [check("file_exists", False, "File not found in R2")],
                        "row_count": 0,
                        "columns": [],
                    })
                    scraper_result["files_found"] += 1
                    continue

                scraper_result["files_found"] += 1
                file_result = validate_file(client, bucket, obj["Key"], file_spec, args.quality)
                scraper_result["files"].append(file_result)

        for fr in scraper_result["files"]:
            for c in fr["checks"]:
                scraper_result["checks_total"] += 1
                if c["passed"]:
                    scraper_result["checks_passed"] += 1
                else:
                    scraper_result["all_passed"] = False
                    any_failed = True

        report["scrapers"].append(scraper_result)

    print_summary(report)
    write_step_summary(report)

    report_key = f"{R2_PREFIX}/monitor/{start.isoformat()}/report.json"
    upload_json(client, bucket, report_key, report)
    print(f"\nReport uploaded to s3://{bucket}/{report_key}")

    if args.update_stats:
        existing = load_existing_stats(client, bucket)
        merged = merge_stats(existing, report)
        stats_key = f"{R2_PREFIX}/monitor/monitor_stats.yml"
        upload_yaml(client, bucket, stats_key, merged)
        print(f"Stats updated at s3://{bucket}/{stats_key}")

    if args.fail_on_error and any_failed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
