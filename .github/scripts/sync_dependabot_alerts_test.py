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

import unittest
from unittest import mock

import sync_dependabot_alerts as sda

REPO = "org/repo"


def alert(n, severity="high", state="open"):
  return {
      "number": n,
      "state": state,
      "html_url": f"https://github.com/{REPO}/security/dependabot/{n}",
      "dependency": {
          "package": {"name": "golang.org/x/net", "ecosystem": "go"},
          "manifest_path": "go.mod",
      },
      "security_vulnerability": {
          "vulnerable_version_range": "< 0.30.0",
          "first_patched_version": {"identifier": "0.30.0"},
      },
      "security_advisory": {
          "severity": severity,
          "cve_id": "CVE-2026-1234",
          "ghsa_id": "GHSA-xxxx-yyyy-zzzz",
          "summary": "HTTP/2 rapid reset",
      },
  }


def issue(number, alert_num, state="open"):
  return {"number": number, "state": state, "body": sda.MARKER.format(alert_num)}


class SyncTest(unittest.TestCase):

  def run_sync(self, open_alerts, issues, alert_details=None):
    """Runs sync() against fakes; returns (summary, list of write calls)."""
    writes = []

    def fake_paginate(url, token):
      return open_alerts if "/dependabot/alerts" in url else issues

    def fake_request(method, url, token, data=None):
      if method == "GET":
        return alert_details[int(url.rsplit("/", 1)[1])], {}
      writes.append((method, url, data))
      return {}, {}

    with mock.patch.object(sda, "_paginate", fake_paginate), mock.patch.object(
        sda, "_request", fake_request
    ):
      return sda.sync(REPO, "alerts-tok", "issues-tok"), writes

  def test_creates_issue_with_labels_and_marker(self):
    summary, writes = self.run_sync([alert(101, "critical")], [])
    self.assertEqual(summary["created"], [101])
    [(method, url, data)] = writes
    self.assertEqual((method, url), ("POST", f"/repos/{REPO}/issues"))
    self.assertEqual(
        data["labels"],
        ["security", "dependabot", "p1", "bug", "severity: critical"],
    )
    self.assertIn(sda.MARKER.format(101), data["body"])
    self.assertTrue(data["title"].startswith("[Dependabot #101][CRITICAL]"))

  def test_dedups_open_issue_and_reopens_closed_issue(self):
    summary, writes = self.run_sync(
        [alert(101), alert(102)], [issue(501, 101), issue(502, 102, "closed")]
    )
    self.assertEqual(summary, {"created": [], "reopened": [102], "closed": []})
    self.assertEqual(
        writes, [("PATCH", f"/repos/{REPO}/issues/502", {"state": "open"})]
    )

  def test_closes_issue_when_alert_fixed_or_dismissed(self):
    for state, reason in (("fixed", "completed"), ("dismissed", "not_planned")):
      with self.subTest(state=state):
        summary, writes = self.run_sync(
            [], [issue(505, 105)], {105: alert(105, state=state)}
        )
        self.assertEqual(summary["closed"], [105])
        (_, comment_url, comment), close = writes
        self.assertEqual(comment_url, f"/repos/{REPO}/issues/505/comments")
        self.assertIn(state, comment["body"])
        self.assertEqual(
            close,
            ("PATCH", f"/repos/{REPO}/issues/505",
             {"state": "closed", "state_reason": reason}),
        )

  def test_paginate_follows_link_cursor_without_page_param(self):
    next_url = f"{sda.API}/repos/{REPO}/dependabot/alerts?after=abc&per_page=100"
    pages = [
        ([{"number": 1}], {"link": f'<{next_url}>; rel="next"'}),
        ([{"number": 2}], {}),
    ]
    with mock.patch.object(sda, "_request", side_effect=pages) as req:
      items = sda._paginate("/start?per_page=100", "tok")
    self.assertEqual(items, [{"number": 1}, {"number": 2}])
    urls = [c.args[1] for c in req.call_args_list]
    self.assertEqual(urls, ["/start?per_page=100", next_url])
    self.assertFalse(any("page=" in u.replace("per_page=", "") for u in urls))


if __name__ == "__main__":
  unittest.main()
