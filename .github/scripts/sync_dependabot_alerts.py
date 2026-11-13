#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Syncs open Critical/High GitHub Dependabot alerts to GitHub Issues.

Creates 1 GitHub Issue per open Dependabot alert matching the target severities
(default: critical, high) so that GitHub Issue Mirror (go/github-issue-mirror)
automatically mirrors each alert into Buganizer component 1225677 and surfaces
it in the Cloud Spanner Migrations TaskFlow inbox (workspace 2565865).

When a tracked Dependabot alert is fixed or dismissed, this script automatically
closes the corresponding GitHub Issue so GitHub Issue Mirror resolves the
Buganizer issue.
"""

import argparse
import json
import logging
import os
import re
import sys
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

GITHUB_API_BASE = "https://api.github.com"
NEXT_LINK_REGEX = re.compile(r'<([^>]+)>;\s*rel="next"')
ALERT_MARKER_TEMPLATE = "<!-- dependabot-alert-number: {} -->"
ALERT_MARKER_REGEX = re.compile(r"<!--\s*dependabot-alert-number:\s*(\d+)\s*-->")
TITLE_FALLBACK_REGEX = re.compile(r"\[Dependabot #(\d+)\]")

TRACKING_LABEL = "dependabot"
DEFAULT_LABELS = ("security", "dependabot", "p1", "bug")

LABEL_DEFINITIONS = {
    "security": {
        "color": "d73a4a",
        "description": "Security vulnerability or advisory",
    },
    "dependabot": {
        "color": "0366d6",
        "description": "Tracked Dependabot security alert",
    },
    "p1": {
        "color": "b60205",
        "description": "Priority P1 (synced to Buganizer P1)",
    },
    "bug": {
        "color": "d73a4a",
        "description": "Something isn't working (synced to Buganizer BUG)",
    },
    "severity: critical": {
        "color": "b60205",
        "description": "Critical severity security vulnerability",
    },
    "severity: high": {
        "color": "d93f0b",
        "description": "High severity security vulnerability",
    },
    "obsolete": {
        "color": "cfd3d7",
        "description": "Dismissed or obsolete alert",
    },
}


class GitHubClient:
  """Minimal GitHub REST API client using Python standard library urllib."""

  def __init__(self, api_base: str = GITHUB_API_BASE):
    self.api_base = api_base.rstrip("/")

  def request(
      self,
      method: str,
      path: str,
      token: str,
      data: dict[str, Any] | None = None,
      params: dict[str, str] | None = None,
  ) -> tuple[Any, dict[str, str]]:
    """Performs an HTTP request against the GitHub REST API."""
    if path.startswith("http://") or path.startswith("https://"):
      url = path
    else:
      url = f"{self.api_base}{path}"
    if params:
      query = urllib.parse.urlencode(params)
      separator = "&" if "?" in url else "?"
      url = f"{url}{separator}{query}"

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "spanner-migration-tool-dependabot-sync",
    }
    body_bytes = None
    if data is not None:
      body_bytes = json.dumps(data).encode("utf-8")
      headers["Content-Type"] = "application/json"

    req = urllib.request.Request(
        url=url, data=body_bytes, headers=headers, method=method
    )
    try:
      with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        parsed = json.loads(raw) if raw else None
        resp_headers = {k.lower(): v for k, v in resp.headers.items()}
        return parsed, resp_headers
    except urllib.error.HTTPError as e:
      err_body = e.read().decode("utf-8", errors="replace")
      raise RuntimeError(
          f"GitHub API {method} {url} failed with HTTP {e.code}: {err_body}"
      ) from e

  def paginate(
      self,
      path: str,
      token: str,
      params: dict[str, str] | None = None,
  ) -> list[dict[str, Any]]:
    """Fetches all pages for a paginated GitHub GET endpoint using Link headers."""
    results: list[dict[str, Any]] = []
    next_url: str | None = path
    next_params: dict[str, str] | None = dict(params or {})
    next_params["per_page"] = "100"

    while next_url:
      items, resp_headers = self.request(
          "GET", next_url, token=token, params=next_params
      )
      next_params = None  # Subsequent requests use the full URL from Link header
      if not items:
        break
      if not isinstance(items, list):
        raise RuntimeError(
            f"Expected list from {next_url}, got: {type(items)}"
        )
      results.extend(items)
      link_header = resp_headers.get("link", "")
      match = NEXT_LINK_REGEX.search(link_header)
      next_url = match.group(1) if match else None
    return results


def extract_alert_number(issue: dict[str, Any]) -> int | None:
  """Extracts the Dependabot alert number from a tracking GitHub Issue."""
  body = issue.get("body") or ""
  match = ALERT_MARKER_REGEX.search(body)
  if match:
    return int(match.group(1))
  title = issue.get("title") or ""
  title_match = TITLE_FALLBACK_REGEX.search(title)
  if title_match:
    return int(title_match.group(1))
  return None


def get_alert_severity(alert: dict[str, Any]) -> str:
  """Returns the lowercase severity of a Dependabot alert."""
  vuln = alert.get("security_vulnerability") or {}
  advisory = alert.get("security_advisory") or {}
  severity = vuln.get("severity") or advisory.get("severity") or "unknown"
  return str(severity).lower()


def format_alert_issue_title(alert: dict[str, Any]) -> str:
  """Builds the deterministic GitHub Issue title for a Dependabot alert."""
  number = alert["number"]
  severity = get_alert_severity(alert).upper()
  dep = alert.get("dependency") or {}
  pkg = (dep.get("package") or {}).get("name") or "unknown-package"
  manifest = dep.get("manifest_path") or "unknown-manifest"
  advisory = alert.get("security_advisory") or {}
  adv_id = advisory.get("cve_id") or advisory.get("ghsa_id") or "advisory"
  summary = advisory.get("summary") or "Security vulnerability"
  return (
      f"[Security][Dependabot #{number}][{severity}] {pkg} in {manifest}"
      f" ({adv_id}): {summary}"
  )[:256]


def format_alert_issue_body(alert: dict[str, Any]) -> str:
  """Builds the markdown body for a Dependabot alert tracking GitHub Issue."""
  number = alert["number"]
  marker = ALERT_MARKER_TEMPLATE.format(number)
  severity = get_alert_severity(alert).upper()
  html_url = alert.get("html_url") or ""
  created_at = alert.get("created_at") or "unknown"

  dep = alert.get("dependency") or {}
  pkg_info = dep.get("package") or {}
  pkg_name = pkg_info.get("name") or "unknown"
  ecosystem = pkg_info.get("ecosystem") or "unknown"
  manifest_path = dep.get("manifest_path") or "unknown"
  scope = dep.get("scope") or "unknown"

  vuln = alert.get("security_vulnerability") or {}
  vuln_range = vuln.get("vulnerable_version_range") or "unknown"
  first_patched = (
      (vuln.get("first_patched_version") or {}).get("identifier")
      or "None available"
  )

  advisory = alert.get("security_advisory") or {}
  ghsa_id = advisory.get("ghsa_id") or "N/A"
  cve_id = advisory.get("cve_id") or "N/A"
  summary = advisory.get("summary") or "No summary provided."

  return f"""{marker}
## Dependabot Security Alert #{number}

An open **{severity}** severity Dependabot alert was detected in this repository.

| Field | Value |
| :--- | :--- |
| **Dependabot Alert** | [Alert #{number}]({html_url}) |
| **Severity** | `{severity}` |
| **Package** | `{pkg_name}` (`{ecosystem}`) |
| **Manifest Path** | `{manifest_path}` (scope: `{scope}`) |
| **CVE ID** | `{cve_id}` |
| **GHSA ID** | `{ghsa_id}` |
| **Vulnerable Versions** | `{vuln_range}` |
| **First Patched Version** | `{first_patched}` |
| **Alert Created At** | `{created_at}` |

### Advisory Summary
{summary}

---
*This issue is automatically managed by the `.github/workflows/sync-dependabot-alerts.yaml` workflow so security alerts are mirrored into Buganizer / TaskFlow via `go/github-issue-mirror`. It will be closed automatically once [Dependabot Alert #{number}]({html_url}) is fixed or dismissed.*
"""


def ensure_labels_exist(
    client: GitHubClient,
    repo: str,
    token: str,
    labels: list[str],
    dry_run: bool = False,
) -> None:
  """Ensures the target labels exist in the repository."""
  existing_labels_raw = client.paginate(f"/repos/{repo}/labels", token=token)
  existing_names = {
      str(lbl.get("name", "")).lower() for lbl in existing_labels_raw
  }

  for label in labels:
    if label.lower() in existing_names:
      continue
    meta = LABEL_DEFINITIONS.get(
        label,
        {"color": "ededed", "description": "Dependabot security tracking label"},
    )
    if dry_run:
      logging.info("[DRY RUN] Would create missing GitHub label: %s", label)
      continue
    try:
      client.request(
          "POST",
          f"/repos/{repo}/labels",
          token=token,
          data={
              "name": label,
              "color": meta["color"],
              "description": meta["description"],
          },
      )
      logging.info("Created GitHub label: %s", label)
    except RuntimeError as e:
      if "422" not in str(e):
        raise
      logging.info("Label %s already exists.", label)


def sync_dependabot_alerts(
    client: GitHubClient,
    repo: str,
    alerts_token: str,
    issues_token: str,
    severities: tuple[str, ...] = ("critical", "high"),
    extra_labels: tuple[str, ...] = (),
    dry_run: bool = False,
) -> dict[str, list[int]]:
  """Syncs open Dependabot alerts to GitHub Issues (1 issue per alert).

  Args:
    client: GitHubClient instance.
    repo: GitHub repository in "owner/name" format.
    alerts_token: GitHub token with Dependabot alerts read permission.
    issues_token: GitHub token with Issues write permission.
    severities: Lowercase alert severities to sync (e.g. ("critical", "high")).
    extra_labels: Any additional custom labels to attach to created issues.
    dry_run: If True, logs planned actions without mutating GitHub Issues.

  Returns:
    Summary dict with lists of alert numbers for "created", "reopened",
    "closed", and "unchanged".
  """
  target_severities = {s.strip().lower() for s in severities if s.strip()}
  severity_param = ",".join(sorted(target_severities))

  # 1. Fetch open Dependabot alerts matching target severities.
  open_alerts_raw = client.paginate(
      f"/repos/{repo}/dependabot/alerts",
      token=alerts_token,
      params={"state": "open", "severity": severity_param},
  )
  open_alerts_by_number: dict[int, dict[str, Any]] = {}
  for alert in open_alerts_raw:
    sev = get_alert_severity(alert)
    if sev in target_severities:
      open_alerts_by_number[int(alert["number"])] = alert

  logging.info(
      "Found %d open Dependabot alert(s) with severity in %s.",
      len(open_alerts_by_number),
      sorted(target_severities),
  )

  # 2. Fetch existing tracking GitHub Issues (both open and closed) with the
  #    `dependabot` label so we never create duplicate issues.
  existing_issues_raw = client.paginate(
      f"/repos/{repo}/issues",
      token=issues_token,
      params={"labels": TRACKING_LABEL, "state": "all"},
  )
  issues_by_alert_number: dict[int, dict[str, Any]] = {}
  for issue in existing_issues_raw:
    # Skip pull requests returned by the /issues endpoint.
    if "pull_request" in issue:
      continue
    alert_num = extract_alert_number(issue)
    if alert_num is not None and alert_num not in issues_by_alert_number:
      issues_by_alert_number[alert_num] = issue

  # 3. Ensure required labels exist on the repository.
  all_needed_labels = list(dict.fromkeys([
      *DEFAULT_LABELS,
      *(f"severity: {s}" for s in sorted(target_severities)),
      "obsolete",
      *extra_labels,
  ]))
  ensure_labels_exist(
      client, repo, token=issues_token, labels=all_needed_labels, dry_run=dry_run
  )

  summary: dict[str, list[int]] = {
      "created": [],
      "reopened": [],
      "closed": [],
      "unchanged": [],
  }

  # 4. Create or reopen 1 GitHub Issue per open Critical/High alert.
  for alert_num in sorted(open_alerts_by_number.keys()):
    alert = open_alerts_by_number[alert_num]
    severity = get_alert_severity(alert)
    issue_labels = list(dict.fromkeys([
        *DEFAULT_LABELS,
        f"severity: {severity}",
        *extra_labels,
    ]))

    existing_issue = issues_by_alert_number.get(alert_num)
    if existing_issue is None:
      title = format_alert_issue_title(alert)
      body = format_alert_issue_body(alert)
      if dry_run:
        logging.info(
            "[DRY RUN] Would create issue for Alert #%d: %s (labels=%s)",
            alert_num,
            title,
            issue_labels,
        )
      else:
        created_issue, _ = client.request(
            "POST",
            f"/repos/{repo}/issues",
            token=issues_token,
            data={"title": title, "body": body, "labels": issue_labels},
        )
        logging.info(
            "Created GitHub Issue #%s for Dependabot Alert #%d (%s)",
            created_issue.get("number"),
            alert_num,
            created_issue.get("html_url"),
        )
      summary["created"].append(alert_num)
    elif existing_issue.get("state") == "closed":
      issue_number = existing_issue["number"]
      if dry_run:
        logging.info(
            "[DRY RUN] Would reopen closed GitHub Issue #%d for still-open"
            " Dependabot Alert #%d",
            issue_number,
            alert_num,
        )
      else:
        client.request(
            "PATCH",
            f"/repos/{repo}/issues/{issue_number}",
            token=issues_token,
            data={"state": "open"},
        )
        logging.info(
            "Reopened GitHub Issue #%d for Dependabot Alert #%d",
            issue_number,
            alert_num,
        )
      summary["reopened"].append(alert_num)
    else:
      summary["unchanged"].append(alert_num)

  # 5. Auto-close open tracking issues whose Dependabot alert is no longer open.
  for alert_num, issue in sorted(issues_by_alert_number.items()):
    if issue.get("state") != "open":
      continue
    if alert_num in open_alerts_by_number:
      continue

    issue_number = issue["number"]
    alert_state = "resolved"
    try:
      alert_detail, _ = client.request(
          "GET",
          f"/repos/{repo}/dependabot/alerts/{alert_num}",
          token=alerts_token,
      )
      alert_state = str(alert_detail.get("state") or "resolved")
    except RuntimeError as e:
      logging.warning(
          "Could not fetch status for Alert #%d (%s); skipping auto-close.",
          alert_num,
          e,
      )
      continue

    if alert_state == "open":
      # Alert is still open, e.g. its severity was downgraded below high.
      continue

    if dry_run:
      logging.info(
          "[DRY RUN] Would close GitHub Issue #%d because Dependabot Alert #%d"
          " state is '%s'",
          issue_number,
          alert_num,
          alert_state,
      )
    else:
      if alert_state in ("dismissed", "auto_dismissed"):
        current_labels = [
            lbl["name"] for lbl in issue.get("labels", []) if "name" in lbl
        ]
        if "obsolete" not in {l.lower() for l in current_labels}:
          current_labels.append("obsolete")
          client.request(
              "PATCH",
              f"/repos/{repo}/issues/{issue_number}",
              token=issues_token,
              data={"labels": current_labels},
          )
      client.request(
          "POST",
          f"/repos/{repo}/issues/{issue_number}/comments",
          token=issues_token,
          data={
              "body": (
                  f"Closing this issue automatically because Dependabot Alert"
                  f" #{alert_num} is now **{alert_state}**."
              )
          },
      )
      client.request(
          "PATCH",
          f"/repos/{repo}/issues/{issue_number}",
          token=issues_token,
          data={"state": "closed"},
      )
      logging.info(
          "Closed GitHub Issue #%d (Dependabot Alert #%d is %s).",
          issue_number,
          alert_num,
          alert_state,
      )
    summary["closed"].append(alert_num)

  return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
  """Parses CLI arguments."""
  parser = argparse.ArgumentParser(
      description="Sync Critical/High Dependabot alerts to GitHub Issues."
  )
  parser.add_argument(
      "--repo",
      default=os.environ.get(
          "GITHUB_REPOSITORY", "GoogleCloudPlatform/spanner-migration-tool"
      ),
      help="GitHub repository in owner/name format.",
  )
  parser.add_argument(
      "--alerts-token",
      default=os.environ.get("DEPENDABOT_ALERTS_TOKEN", ""),
      help="GitHub token with Dependabot alerts read permission.",
  )
  parser.add_argument(
      "--issues-token",
      default=os.environ.get("GITHUB_TOKEN", ""),
      help="GitHub token with Issues write permission.",
  )
  parser.add_argument(
      "--severities",
      default="critical,high",
      help="Comma-separated list of severities to sync (default: critical,high).",
  )
  parser.add_argument(
      "--extra-labels",
      default="",
      help="Optional comma-separated list of additional GitHub labels to add.",
  )
  parser.add_argument(
      "--dry-run",
      action="store_true",
      help="Log actions without creating or modifying GitHub Issues.",
  )
  return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
  logging.basicConfig(
      level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
  )
  args = parse_args(argv)

  alerts_token = args.alerts_token or args.issues_token
  issues_token = args.issues_token or args.alerts_token
  if not alerts_token:
    logging.error(
        "A GitHub token is required via --alerts-token /"
        " DEPENDABOT_ALERTS_TOKEN."
    )
    return 1

  severities = tuple(
      s.strip().lower() for s in args.severities.split(",") if s.strip()
  )
  extra_labels = tuple(
      l.strip() for l in args.extra_labels.split(",") if l.strip()
  )

  client = GitHubClient()
  try:
    summary = sync_dependabot_alerts(
        client=client,
        repo=args.repo,
        alerts_token=alerts_token,
        issues_token=issues_token,
        severities=severities,
        extra_labels=extra_labels,
        dry_run=args.dry_run,
    )
    summary_json = json.dumps(summary)
    logging.info("Sync complete: %s", summary_json)
    print(f"::notice title=Dependabot Sync Summary::{summary_json}")
    return 0
  except Exception as e:
    print(f"::error title=Dependabot Sync Error::{e}")
    raise


if __name__ == "__main__":
  sys.exit(main())
