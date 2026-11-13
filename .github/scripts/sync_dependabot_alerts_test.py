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
"""Unit tests for sync_dependabot_alerts.py."""

from typing import Any
import unittest

import sync_dependabot_alerts


def _make_alert(
    number: int,
    severity: str = "high",
    state: str = "open",
    pkg_name: str = "golang.org/x/net",
    ecosystem: str = "gomod",
    manifest_path: str = "go.mod",
    cve_id: str = "CVE-2026-1234",
    ghsa_id: str = "GHSA-xxxx-yyyy-zzzz",
) -> dict[str, Any]:
  return {
      "number": number,
      "state": state,
      "html_url": f"https://github.com/GoogleCloudPlatform/spanner-migration-tool/security/dependabot/{number}",
      "created_at": "2026-11-13T00:00:00Z",
      "dependency": {
          "package": {"name": pkg_name, "ecosystem": ecosystem},
          "manifest_path": manifest_path,
          "scope": "runtime",
      },
      "security_vulnerability": {
          "severity": severity,
          "vulnerable_version_range": "< 0.30.0",
          "first_patched_version": {"identifier": "0.30.0"},
      },
      "security_advisory": {
          "severity": severity,
          "cve_id": cve_id,
          "ghsa_id": ghsa_id,
          "summary": "HTTP/2 rapid reset vulnerability",
      },
  }


class FakeGitHubClient(sync_dependabot_alerts.GitHubClient):
  """In-memory fake GitHub client for testing sync_dependabot_alerts."""

  def __init__(
      self,
      open_alerts: list[dict[str, Any]],
      all_alerts_by_num: dict[int, dict[str, Any]] | None = None,
      existing_issues: list[dict[str, Any]] | None = None,
      existing_labels: list[str] | None = None,
  ):
    super().__init__()
    self.open_alerts = open_alerts
    self.all_alerts_by_num = all_alerts_by_num or {
        a["number"]: a for a in open_alerts
    }
    self.issues = list(existing_issues or [])
    self.labels = [{"name": l} for l in (existing_labels or [])]
    self.requests: list[tuple[str, str, Any]] = []

  def paginate(
      self,
      path: str,
      token: str,
      params: dict[str, str] | None = None,
  ) -> list[dict[str, Any]]:
    if path.endswith("/dependabot/alerts"):
      return list(self.open_alerts)
    if path.endswith("/issues"):
      return list(self.issues)
    if path.endswith("/labels"):
      return list(self.labels)
    raise AssertionError(f"Unexpected paginate path: {path}")

  def request(
      self,
      method: str,
      path: str,
      token: str,
      data: dict[str, Any] | None = None,
      params: dict[str, str] | None = None,
  ) -> tuple[Any, dict[str, str]]:
    self.requests.append((method, path, data))
    if method == "POST" and path.endswith("/labels"):
      self.labels.append({"name": data["name"]})
      return data, {}
    if method == "POST" and path.endswith("/issues"):
      new_issue = {
          "number": 1000 + len(self.issues) + 1,
          "state": "open",
          "title": data["title"],
          "body": data["body"],
          "labels": [{"name": l} for l in data.get("labels", [])],
          "html_url": "https://github.com/GoogleCloudPlatform/spanner-migration-tool/issues/1001",
      }
      self.issues.append(new_issue)
      return new_issue, {}
    if method == "GET" and path.endswith("/dependabot/alerts"):
      if params and params.get("state") == "fixed":
        return [
            a
            for a in self.all_alerts_by_num.values()
            if a.get("state") == "fixed"
        ][:1], {}
      return list(self.open_alerts), {}
    if method == "GET" and "/dependabot/alerts/" in path:
      alert_num = int(path.rsplit("/", 1)[-1])
      return self.all_alerts_by_num[alert_num], {}
    if method == "PATCH" and "/issues/" in path:
      issue_num = int(path.rsplit("/", 1)[-1])
      for issue in self.issues:
        if issue["number"] == issue_num:
          if "state" in data:
            issue["state"] = data["state"]
          if "labels" in data:
            issue["labels"] = [{"name": l} for l in data["labels"]]
          return issue, {}
    if method == "POST" and path.endswith("/comments"):
      return {"id": 1}, {}
    raise AssertionError(f"Unexpected request: {method} {path}")


class SyncDependabotAlertsTest(unittest.TestCase):

  def test_creates_one_issue_per_critical_or_high_alert_with_labels(self):
    alerts = [
        _make_alert(101, severity="critical", pkg_name="axios", ecosystem="npm", manifest_path="ui/package-lock.json"),
        _make_alert(102, severity="high", pkg_name="golang.org/x/net"),
        _make_alert(103, severity="medium", pkg_name="lodash"),
    ]
    client = FakeGitHubClient(open_alerts=alerts)

    summary = sync_dependabot_alerts.sync_dependabot_alerts(
        client=client,
        repo="GoogleCloudPlatform/spanner-migration-tool",
        alerts_token="alert-tok",
        issues_token="issue-tok",
        severities=("critical", "high"),
        extra_labels=("smt-security",),
    )

    self.assertEqual(summary["created"], [101, 102])
    self.assertEqual(summary["reopened"], [])
    self.assertEqual(summary["closed"], [])
    self.assertEqual(len(client.issues), 2)

    issue_101 = client.issues[0]
    self.assertIn("[Security][Dependabot #101][CRITICAL]", issue_101["title"])
    self.assertIn("<!-- dependabot-alert-number: 101 -->", issue_101["body"])
    labels_101 = [l["name"] for l in issue_101["labels"]]
    self.assertEqual(
        labels_101,
        ["security", "dependabot", "p1", "bug", "severity: critical", "smt-security"],
    )

  def test_deduplicates_existing_open_issue_and_reopens_closed_issue(self):
    alert_101 = _make_alert(101, severity="critical")
    alert_102 = _make_alert(102, severity="high")
    existing_issues = [
        {
            "number": 501,
            "state": "open",
            "title": sync_dependabot_alerts.format_alert_issue_title(alert_101),
            "body": sync_dependabot_alerts.format_alert_issue_body(alert_101),
            "labels": [{"name": "dependabot"}],
        },
        {
            "number": 502,
            "state": "closed",
            "title": sync_dependabot_alerts.format_alert_issue_title(alert_102),
            "body": sync_dependabot_alerts.format_alert_issue_body(alert_102),
            "labels": [{"name": "dependabot"}],
        },
    ]
    client = FakeGitHubClient(
        open_alerts=[alert_101, alert_102],
        existing_issues=existing_issues,
    )

    summary = sync_dependabot_alerts.sync_dependabot_alerts(
        client=client,
        repo="GoogleCloudPlatform/spanner-migration-tool",
        alerts_token="alert-tok",
        issues_token="issue-tok",
    )

    self.assertEqual(summary["created"], [])
    self.assertEqual(summary["unchanged"], [101])
    self.assertEqual(summary["reopened"], [102])
    self.assertEqual(client.issues[1]["state"], "open")

  def test_auto_closes_issue_when_alert_is_fixed_or_dismissed(self):
    fixed_alert = _make_alert(105, severity="high", state="dismissed")
    existing_issues = [
        {
            "number": 505,
            "state": "open",
            "title": sync_dependabot_alerts.format_alert_issue_title(fixed_alert),
            "body": sync_dependabot_alerts.format_alert_issue_body(fixed_alert),
            "labels": [{"name": "dependabot"}, {"name": "p1"}],
        }
    ]
    client = FakeGitHubClient(
        open_alerts=[],
        all_alerts_by_num={105: fixed_alert},
        existing_issues=existing_issues,
    )

    summary = sync_dependabot_alerts.sync_dependabot_alerts(
        client=client,
        repo="GoogleCloudPlatform/spanner-migration-tool",
        alerts_token="alert-tok",
        issues_token="issue-tok",
    )

    self.assertEqual(summary["closed"], [105])
    self.assertEqual(client.issues[0]["state"], "closed")
    closed_labels = [l["name"] for l in client.issues[0]["labels"]]
    self.assertIn("obsolete", closed_labels)

  def test_cursor_pagination_follows_link_header_without_page_param(self):
    calls: list[tuple[str, dict[str, str] | None]] = []

    class CursorClient(sync_dependabot_alerts.GitHubClient):

      def request(self, method, path, token, data=None, params=None):
        if params and "page" in params:
          raise AssertionError("`page` parameter must not be sent")
        calls.append((path, params))
        if len(calls) == 1:
          return [{"number": 1}], {
              "link": (
                  '<https://api.github.com/repos/org/repo/dependabot/alerts?after=cursor123&per_page=100>;'
                  ' rel="next"'
              )
          }
        return [{"number": 2}], {}

    client = CursorClient()
    items = client.paginate(
        "/repos/org/repo/dependabot/alerts",
        token="tok",
        params={"state": "open", "severity": "critical,high"},
    )
    self.assertEqual([i["number"] for i in items], [1, 2])
    self.assertEqual(
        calls[0],
        (
            "/repos/org/repo/dependabot/alerts",
            {"state": "open", "severity": "critical,high", "per_page": "100"},
        ),
    )
    self.assertEqual(
        calls[1],
        (
            "https://api.github.com/repos/org/repo/dependabot/alerts?after=cursor123&per_page=100",
            None,
        ),
    )

  def test_simulate_open_alert_creates_issue_from_fixed_alert(self):
    fixed_alert = _make_alert(150, severity="high", state="fixed")
    client = FakeGitHubClient(
        open_alerts=[],
        all_alerts_by_num={150: fixed_alert},
    )

    summary = sync_dependabot_alerts.sync_dependabot_alerts(
        client=client,
        repo="GoogleCloudPlatform/spanner-migration-tool",
        alerts_token="alert-tok",
        issues_token="issue-tok",
        simulate_open_alert=True,
    )

    self.assertEqual(summary["created"], [150])
    self.assertEqual(len(client.issues), 1)
    self.assertEqual(client.issues[0]["state"], "open")


if __name__ == "__main__":
  unittest.main()
