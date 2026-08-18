"""
Resolve scraper pipeline metadata from site/registry config and GitHub Actions API.

The Schema Monitor workflow must not appear as the site's CI workflow in the dashboard;
sites declare their scraper workflows in registry.yml or site.yml.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

log = logging.getLogger("monitor")

MONITOR_WORKFLOW_NAMES = frozenset({
    "Schema Monitor",
    "R2 Schema Monitor",
    "R2 Excel Schema Monitor",
    "R2 CSV Monitor",
    "KCSB R2 Schema Monitor",
    "Monitor Hub — Aggregate All Sites",
    "Monitor Categories",
    "monitor",
})

_SCHEDULE_LOOKBACK_DAYS = {
    "daily": 2,
    "every_2_days": 4,
    "weekly": 8,
    "biweekly": 15,
    "monthly": 35,
    "quarterly": 125,
}


def is_monitor_workflow(name: Optional[str]) -> bool:
    if not name:
        return False
    if name in MONITOR_WORKFLOW_NAMES:
        return True
    if name.startswith("Monitor Hub"):
        return True
    lower = name.lower()
    if "schema monitor" in lower or lower.endswith(" r2 monitor"):
        return True
    return False


def resolve_workflow_names(site: Dict) -> List[str]:
    """Ordered scraper workflow display names (same repo unless entry specifies repo)."""
    return [e["name"] for e in parse_workflow_entries(site)]


def parse_workflow_entries(site: Dict) -> List[Dict[str, str]]:
    """
    Normalize workflows config to [{name, owner, repo}, ...].

    Supports:
      workflows: ["Batch A", "Batch B-1"]
      workflows:
        - name: "CF Daily Scrapers - All Categories (Cloudflare R2)"
          repo: Codinity
    """
    owner = (site.get("github_username") or site.get("github_owner") or "").strip()
    default_repo = (site.get("repo") or "").strip()
    raw: Union[str, List[Any], None] = site.get("workflows")

    if raw is None:
        single = site.get("workflow_name")
        if single and not is_monitor_workflow(str(single)):
            if owner and default_repo:
                return [{"name": str(single), "owner": owner, "repo": default_repo}]
        return []

    items: List[Any]
    if isinstance(raw, str):
        items = [p.strip() for p in raw.replace("→", ",").split(",") if p.strip()]
    elif isinstance(raw, list):
        items = raw
    else:
        return []

    entries: List[Dict[str, str]] = []
    for item in items:
        if isinstance(item, str):
            name = item.strip()
            if not name or is_monitor_workflow(name):
                continue
            if not owner or not default_repo:
                continue
            entries.append({"name": name, "owner": owner, "repo": default_repo})
        elif isinstance(item, dict):
            name = (item.get("name") or item.get("workflow") or "").strip()
            if not name or is_monitor_workflow(name):
                continue
            entry_owner = (item.get("owner") or item.get("github_username") or owner).strip()
            entry_repo = (item.get("repo") or default_repo).strip()
            if not entry_owner or not entry_repo:
                continue
            entries.append({"name": name, "owner": entry_owner, "repo": entry_repo})
    return entries


def format_workflow_label(names: List[str]) -> str:
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    return " → ".join(names)


def merge_registry_site(site: Dict, registry: Optional[Dict]) -> Dict:
    """Overlay registry fields onto site config for metadata resolution."""
    if not registry:
        return site

    folder = site.get("folder")
    site_id = site.get("site_id")
    reg_row: Dict = {}
    for row in registry.get("sites", []):
        if folder and row.get("folder") == folder:
            reg_row = row
            break
        if site_id and row.get("site_id") == site_id:
            reg_row = row
            break

    if not reg_row:
        return site

    merged = dict(site)
    for key in (
        "github_username",
        "repo",
        "run_place",
        "schedule",
        "workflows",
        "workflow_name",
        "github_gmail",
    ):
        if merged.get(key) in (None, "", []):
            val = reg_row.get(key)
            if val not in (None, "", []):
                merged[key] = val
    return merged


def enrich_site_from_env(site: Dict) -> Dict:
    """Fill github_username/repo from GITHUB_REPOSITORY when meta omits them."""
    merged = dict(site)
    owner = (merged.get("github_username") or merged.get("github_owner") or "").strip()
    repo = (merged.get("repo") or "").strip()
    if owner and repo:
        return merged

    gh_repo = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    if "/" not in gh_repo:
        return merged

    env_owner, env_repo = gh_repo.split("/", 1)
    if not owner:
        merged["github_username"] = env_owner
    if not repo:
        merged["repo"] = env_repo
    return merged


def _lookback_start(partition_date: str, schedule: Optional[str]) -> datetime:
    sched = (schedule or "daily").lower().replace(" ", "_").replace("-", "_")
    days = _SCHEDULE_LOOKBACK_DAYS.get(sched, 2)
    try:
        start = datetime.strptime(partition_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        start = datetime.now(timezone.utc)
    return start - timedelta(days=days)


def _github_request(url: str, token: str) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "schema-monitor",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _parse_github_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _run_duration_sec(run: Dict) -> int:
    started = _parse_github_dt(run.get("run_started_at"))
    finished = _parse_github_dt(run.get("updated_at"))
    if started and finished:
        return max(0, int((finished - started).total_seconds()))
    return 0


def _workflow_name_map(owner: str, repo: str, token: str) -> Dict[str, int]:
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows?per_page=100"
    data = _github_request(url, token)
    mapping: Dict[str, int] = {}
    for wf in data.get("workflows", []):
        name = wf.get("name")
        wf_id = wf.get("id")
        if name and wf_id:
            mapping[str(name)] = int(wf_id)
    return mapping


def _latest_run_for_workflow(
    owner: str,
    repo: str,
    workflow_id: int,
    token: str,
    not_before: datetime,
) -> Optional[Dict]:
    url = (
        f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/"
        f"{workflow_id}/runs?per_page=20&exclude_pull_requests=true"
    )
    data = _github_request(url, token)
    for run in data.get("workflow_runs", []):
        if run.get("status") != "completed":
            continue
        started = _parse_github_dt(run.get("run_started_at"))
        if started and started >= not_before:
            return run
    return None


def _discover_latest_scraper_run(
    owner: str,
    repo: str,
    token: str,
    not_before: datetime,
) -> Optional[Dict]:
    """
    When site meta has no workflows list, pick the newest completed non-monitor
    workflow run in the lookback window.
    """
    url = (
        f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
        f"?per_page=30&exclude_pull_requests=true"
    )
    data = _github_request(url, token)
    for run in data.get("workflow_runs", []):
        name = run.get("name") or run.get("display_title") or ""
        if is_monitor_workflow(str(name)):
            continue
        if run.get("status") != "completed":
            continue
        started = _parse_github_dt(run.get("run_started_at"))
        if started and started >= not_before:
            return run
    return None


def _run_detail_from_api(run: Dict, owner: str, repo: str, wf_name: str) -> Dict[str, Any]:
    return {
        "name": wf_name,
        "owner": owner,
        "repo": repo,
        "run_id": run.get("id"),
        "run_number": run.get("run_number"),
        "conclusion": run.get("conclusion"),
        "status": run.get("status"),
        "duration_sec": _run_duration_sec(run),
        "run_started_at": run.get("run_started_at"),
        "updated_at": run.get("updated_at"),
        "html_url": run.get("html_url"),
    }


def _pipeline_status(conclusions: List[Optional[str]]) -> str:
    if not conclusions:
        return "unknown"
    for conclusion in conclusions:
        if conclusion in (None, "cancelled"):
            continue
        if conclusion not in ("success", "skipped"):
            return "failure"
    return "success"


def fetch_pipeline_github_meta(
    site: Dict,
    partition_date: str,
    token: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Fetch latest GitHub Actions runs for configured scraper workflows."""
    site = enrich_site_from_env(site)
    entries = parse_workflow_entries(site)

    token = (token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if not token:
        log.info("Skipping GitHub workflow lookup — no GITHUB_TOKEN")
        return None

    not_before = _lookback_start(partition_date, site.get("schedule"))
    owner = (site.get("github_username") or site.get("github_owner") or "").strip()
    repo = (site.get("repo") or "").strip()

    # No workflows declared — discover newest non-monitor completed run.
    if not entries:
        if not owner or not repo:
            log.info("Skipping GitHub workflow lookup — no owner/repo and no workflows configured")
            return None
        try:
            run = _discover_latest_scraper_run(owner, repo, token, not_before)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            log.warning(f"GitHub run discovery failed for {owner}/{repo}: {exc}")
            return None
        if not run:
            log.info(
                f"No recent non-monitor run for {owner}/{repo} since {not_before.isoformat()}"
            )
            return None
        wf_name = str(run.get("name") or "scraper")
        detail = _run_detail_from_api(run, owner, repo, wf_name)
        return {
            "run_place": "github",
            "workflow_name": wf_name,
            "workflow_status": _pipeline_status([detail.get("conclusion")]),
            "duration_sec": detail.get("duration_sec") or 0,
            "workflow_run_id": str(detail["run_id"]) if detail.get("run_id") else None,
            "workflow_run_number": detail.get("run_number"),
            "github_repository": f"{owner}/{repo}",
            "workflows": [detail],
            "source": "github_api_discovered",
        }

    # Cache workflow id maps per repo
    id_cache: Dict[str, Dict[str, int]] = {}
    runs_detail: List[Dict[str, Any]] = []

    for entry in entries:
        entry_owner = entry["owner"]
        entry_repo = entry["repo"]
        wf_name = entry["name"]
        cache_key = f"{entry_owner}/{entry_repo}"

        if cache_key not in id_cache:
            try:
                id_cache[cache_key] = _workflow_name_map(entry_owner, entry_repo, token)
            except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
                log.warning(f"GitHub workflow list failed for {cache_key}: {exc}")
                id_cache[cache_key] = {}

        wf_id = id_cache[cache_key].get(wf_name)
        if not wf_id:
            log.warning(f"Workflow not found in {cache_key}: {wf_name!r}")
            continue
        try:
            run = _latest_run_for_workflow(entry_owner, entry_repo, wf_id, token, not_before)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
            log.warning(f"GitHub run lookup failed for {cache_key} / {wf_name}: {exc}")
            continue
        if not run:
            log.info(f"No recent run for {cache_key} / {wf_name} since {not_before.isoformat()}")
            continue
        runs_detail.append(_run_detail_from_api(run, entry_owner, entry_repo, wf_name))

    if not runs_detail:
        return None

    configured_names = [e["name"] for e in entries]
    found_names = [r["name"] for r in runs_detail]
    label_names = found_names if len(found_names) == len(configured_names) else configured_names
    conclusions = [r.get("conclusion") for r in runs_detail]
    last_run = runs_detail[-1]
    primary_owner = entries[0]["owner"]
    primary_repo = entries[0]["repo"]

    return {
        "run_place": "github",
        "workflow_name": format_workflow_label(label_names),
        "workflow_status": _pipeline_status(conclusions),
        "duration_sec": sum(r.get("duration_sec") or 0 for r in runs_detail),
        "workflow_run_id": str(last_run["run_id"]) if last_run.get("run_id") else None,
        "workflow_run_number": last_run.get("run_number"),
        "github_repository": f"{primary_owner}/{primary_repo}",
        "workflows": runs_detail,
        "source": "github_api",
    }


def build_scraper_run_meta(
    site: Dict,
    partition_date: str,
    monitor_started_at: datetime,
    validation_passed: bool,
    fallback_duration_sec: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Build github_run metadata for report.json / dashboard.

    Prefers scraper pipeline runs from GitHub API; never labels the monitor workflow
    as the site's primary workflow.
    """
    site = enrich_site_from_env(site)
    pipeline = fetch_pipeline_github_meta(site, partition_date)

    monitor_duration = max(0, int((datetime.utcnow() - monitor_started_at).total_seconds()))
    monitor_meta: Dict[str, Any] = {
        "run_place": (site.get("run_place") or "github").strip().lower(),
        "workflow_name": os.environ.get("GITHUB_WORKFLOW", "Schema Monitor"),
        "workflow_status": "success" if validation_passed else "failure",
        "duration_sec": monitor_duration,
        "started_at": monitor_started_at.isoformat() + "Z",
        "finished_at": datetime.utcnow().isoformat() + "Z",
    }
    if os.environ.get("GITHUB_ACTIONS") == "true":
        monitor_meta.update({
            "workflow_run_id": os.environ.get("GITHUB_RUN_ID", ""),
            "workflow_run_number": int(os.environ.get("GITHUB_RUN_NUMBER") or 0),
            "github_repository": os.environ.get("GITHUB_REPOSITORY", ""),
        })

    if pipeline:
        result = dict(pipeline)
        result["monitor_run"] = monitor_meta
        github_gmail = (site.get("github_gmail") or site.get("github_email") or "").strip()
        if github_gmail:
            result["github_gmail"] = github_gmail
        return result

    configured = resolve_workflow_names(site)
    fallback_name = format_workflow_label(configured) if configured else None
    if not fallback_name:
        legacy = site.get("workflow_name")
        if legacy and not is_monitor_workflow(str(legacy)):
            fallback_name = str(legacy)

    if fallback_duration_sec is not None and fallback_duration_sec > 0:
        duration_sec = int(fallback_duration_sec)
        duration_source = "request_metrics"
    else:
        duration_sec = monitor_duration
        duration_source = "monitor_run"

    run_place = (site.get("run_place") or "github").strip().lower()
    result = {
        "run_place": run_place,
        "workflow_name": fallback_name or "—",
        "workflow_status": None,
        "duration_sec": duration_sec,
        "duration_source": duration_source,
        "monitor_run": monitor_meta,
        "source": "registry_fallback",
    }
    github_gmail = (site.get("github_gmail") or site.get("github_email") or "").strip()
    if github_gmail:
        result["github_gmail"] = github_gmail
    return result
