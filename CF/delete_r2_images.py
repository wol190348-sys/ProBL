#!/usr/bin/env python3
"""
Delete existing Bleems images from Cloudflare R2.

Targets only image objects under the site prefix (default: bleems-data/).
CSVs, JSON, YAML, and monitor artifacts are never deleted.

Dry-run by default. Pass --execute to actually remove objects.

Environment (same secrets as the R2 scraper workflow):
  CF_R2_ACCESS_KEY_ID
  CF_R2_SECRET_ACCESS_KEY
  CF_R2_ENDPOINT_URL
  CF_R2_BUCKET_NAME
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

R2_PREFIX = "bleems-data"
DELETE_BATCH_SIZE = 1000
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff", ".tif", ".svg", ".ico"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete existing Bleems images from Cloudflare R2"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete matching objects (default is a dry-run preview)",
    )
    parser.add_argument(
        "--prefix",
        default=R2_PREFIX,
        help=f"Site prefix inside the bucket (default: {R2_PREFIX})",
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"{name} env var is required")
    return value


def build_r2_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=require_env("CF_R2_ENDPOINT_URL"),
        aws_access_key_id=require_env("CF_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=require_env("CF_R2_SECRET_ACCESS_KEY"),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip("/")
    return f"{prefix}/" if prefix else ""


def is_image_key(key: str) -> bool:
    if key.endswith("/"):
        return False
    lower = key.lower()
    _, ext = os.path.splitext(lower)
    if ext in IMAGE_EXTS:
        return True
    return "/images/" in f"/{lower}"


def format_bytes(size_bytes: int) -> str:
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024.0 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024.0
    return f"{size_bytes} B"


def list_image_objects(client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    prefix = normalize_prefix(prefix)
    images: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if is_image_key(key):
                images.append({"Key": key, "Size": int(obj.get("Size", 0) or 0)})
    return images


def delete_in_batches(client: Any, bucket: str, keys: list[str]) -> tuple[int, int]:
    deleted = 0
    errors = 0
    for start in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = [{"Key": key} for key in keys[start : start + DELETE_BATCH_SIZE]]
        try:
            resp = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": batch, "Quiet": False},
            )
        except ClientError as exc:
            log.error("Batch delete failed: %s", exc)
            errors += len(batch)
            continue

        deleted += len(resp.get("Deleted", []))
        for err in resp.get("Errors", []):
            errors += 1
            log.error(
                "Failed to delete %s: %s (%s)",
                err.get("Key"),
                err.get("Message"),
                err.get("Code"),
            )
        log.info(
            "Deleted batch %s–%s of %s",
            start + 1,
            min(start + DELETE_BATCH_SIZE, len(keys)),
            len(keys),
        )
    return deleted, errors


def write_step_summary(
    *,
    bucket: str,
    prefix: str,
    execute: bool,
    images: list[dict[str, Any]],
    deleted: int,
    errors: int,
) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    total_bytes = sum(obj["Size"] for obj in images)
    mode = "EXECUTE" if execute else "DRY RUN"
    lines = [
        "## Delete Cloudflare R2 images",
        "",
        f"- **Mode:** {mode}",
        f"- **Bucket:** `{bucket}`",
        f"- **Prefix:** `{prefix}`",
        f"- **Images found:** {len(images)}",
        f"- **Size:** {format_bytes(total_bytes)}",
    ]
    if execute:
        lines.extend([
            f"- **Deleted:** {deleted}",
            f"- **Errors:** {errors}",
        ])
    else:
        lines.append("- No objects were deleted (dry run).")

    if images:
        lines.extend(["", "### Sample keys", ""])
        for obj in images[:25]:
            lines.append(f"- `{obj['Key']}` ({format_bytes(obj['Size'])})")
        if len(images) > 25:
            lines.append(f"- … and {len(images) - 25} more")

    with open(summary_path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()
    bucket = require_env("CF_R2_BUCKET_NAME")
    prefix = args.prefix.strip("/") or R2_PREFIX
    client = build_r2_client()

    log.info("Bucket : %s", bucket)
    log.info("Prefix : %s/", prefix)
    log.info("Mode   : %s", "EXECUTE (delete)" if args.execute else "DRY RUN")

    images = list_image_objects(client, bucket, prefix)
    total_bytes = sum(obj["Size"] for obj in images)
    log.info("Found %s image object(s) totaling %s", len(images), format_bytes(total_bytes))

    for obj in images[:20]:
        log.info("  %s  (%s)", obj["Key"], format_bytes(obj["Size"]))
    if len(images) > 20:
        log.info("  … and %s more", len(images) - 20)

    deleted = 0
    errors = 0
    if not images:
        log.info("Nothing to delete.")
    elif args.execute:
        deleted, errors = delete_in_batches(
            client, bucket, [obj["Key"] for obj in images]
        )
        log.info("Deleted %s object(s); %s error(s)", deleted, errors)
    else:
        log.info("Dry run — pass --execute to delete these objects.")

    write_step_summary(
        bucket=bucket,
        prefix=prefix,
        execute=args.execute,
        images=images,
        deleted=deleted,
        errors=errors,
    )
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
