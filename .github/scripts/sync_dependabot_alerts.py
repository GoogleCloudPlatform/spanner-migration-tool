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
"""Syncs open Critical/High Dependabot alerts to GitHub Issues.

Creates one GitHub Issue per open alert (so GitHub Issue Mirror can track it),
reopens the issue if the alert is open again, and closes it with a comment once
the alert is fixed or dismissed.

Env: GITHUB_REPOSITORY, DEPENDABOT_ALERTS_TOKEN (alerts read), GITHUB_TOKEN
(issues write).
"""

import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request

API = "https://api.github.com"
SEVERITIES = ("critical", "high")
LABELS = ("security", "dependabot", "p1", "bug")
MARKER = "<!-- dependabot-alert-number: {} -->"
MARKER_RE = re.compile(r"<!-- dependabot-alert-number: (\d+) -->")
NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


def _request(method, url, token, data=None):
  """Calls the GitHub REST API. Returns (parsed JSON body, headers)."""
  if not url.startswith("https://"):
    url = API + url
  req = urllib.request.Request(
      url,
      data=json.dumps(data).encode() if data is not None else None,
      method=method,
      headers={
          "Accept": "application/vnd.github+json",
          "Authorization": f"Bearer {token}",
          "X-GitHub-Api-Version": "2022-11-28",
      },
  )
  try:
    with urllib.request.urlopen(req, timeout=30) as resp:
      raw = resp.read()
      return (json.loads(raw) if raw else None), resp.headers
  except urllib.error.HTTPError as e:
    raise RuntimeError(f"{method} {url} -> HTTP {e.code}: {e.read()!r}") from e


def _paginate(url, token):
  """GETs every page by following Link rel="next" (alerts API rejects page=)."""
  items = []
  while url:
    page, headers = _request("GET", url, token)
    items.extend(page or [])
    match = NEXT_RE.search(headers.get("link") or "")
    url = match.group(1) if match else None
  return items


def issue_title(alert):
  adv = alert["security_advisory"]
  pkg = alert["dependency"]["package"]["name"]
  sev = adv["severity"].upper()
  return f"[Dependabot #{alert['number']}][{sev}] {pkg}: {adv['summary']}"[:256]


def issue_body(alert):
  n = alert["number"]
  adv = alert["security_advisory"]
  vuln = alert["security_vulnerability"]
  dep = alert["dependency"]
  patched = (vuln.get("first_patched_version") or {}).get("identifier")
  return f"""{MARKER.format(n)}
| Field | Value |
| :--- | :--- |
| Alert | [#{n}]({alert['html_url']}) |
| Severity | `{adv['severity'].upper()}` |
| Package | `{dep['package']['name']}` (`{dep['package']['ecosystem']}`) |
| Manifest | `{dep['manifest_path']}` |
| CVE / GHSA | `{adv.get('cve_id') or 'N/A'}` / `{adv['ghsa_id']}` |
| Vulnerable versions | `{vuln['vulnerable_version_range']}` |
| First patched version | `{patched or 'None'}` |

{adv['summary']}
"""


def sync(repo, alerts_token, issues_token):
  """Syncs alerts to issues. Returns alert numbers per action."""
  alerts = {
      a["number"]: a
      for a in _paginate(
          f"/repos/{repo}/dependabot/alerts?state=open"
          f"&severity={','.join(SEVERITIES)}&per_page=100",
          alerts_token,
      )
  }
  issues = {}
  for issue in _paginate(
      f"/repos/{repo}/issues?labels=dependabot&state=all&per_page=100",
      issues_token,
  ):
    match = MARKER_RE.search(issue.get("body") or "")
    if match and "pull_request" not in issue:
      issues.setdefault(int(match.group(1)), issue)

  summary = {"created": [], "reopened": [], "closed": []}
  for n, alert in sorted(alerts.items()):
    issue = issues.get(n)
    if issue is None:
      labels = [*LABELS, f"severity: {alert['security_advisory']['severity']}"]
      _request(
          "POST",
          f"/repos/{repo}/issues",
          issues_token,
          {"title": issue_title(alert), "body": issue_body(alert),
           "labels": labels},
      )
      summary["created"].append(n)
    elif issue["state"] == "closed":
      _request(
          "PATCH",
          f"/repos/{repo}/issues/{issue['number']}",
          issues_token,
          {"state": "open"},
      )
      summary["reopened"].append(n)

  for n, issue in sorted(issues.items()):
    if issue["state"] != "open" or n in alerts:
      continue
    try:
      state = _request(
          "GET", f"/repos/{repo}/dependabot/alerts/{n}", alerts_token
      )[0]["state"]
    except RuntimeError as e:
      logging.warning("Skipping alert #%d: %s", n, e)
      continue
    if state == "open":  # E.g. severity downgraded below high.
      continue
    url = f"/repos/{repo}/issues/{issue['number']}"
    _request(
        "POST",
        f"{url}/comments",
        issues_token,
        {"body": f"Closing automatically: Dependabot alert #{n} is **{state}**."},
    )
    reason = "not_planned" if "dismissed" in state else "completed"
    _request(
        "PATCH", url, issues_token, {"state": "closed", "state_reason": reason}
    )
    summary["closed"].append(n)
  return summary


def main():
  logging.basicConfig(level=logging.INFO)
  env = {
      k: os.environ.get(k)
      for k in ("GITHUB_REPOSITORY", "DEPENDABOT_ALERTS_TOKEN", "GITHUB_TOKEN")
  }
  missing = [k for k, v in env.items() if not v]
  if missing:
    sys.exit(f"Missing required env: {', '.join(missing)}")
  summary = json.dumps(sync(*env.values()))
  logging.info("Sync complete: %s", summary)
  print(f"::notice title=Dependabot Sync Summary::{summary}")


if __name__ == "__main__":
  main()
