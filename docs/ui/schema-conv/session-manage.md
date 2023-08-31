---
layout: default
title: Session Management
parent: Schema Conversion Workspace
grand_parent: SMT UI
nav_order: 7
---

# Session Management
{: .no_toc }

Schema conversion operations performed in Spanner migration tool are stored in a `session.json` file. This file can be used to load/restore schema modification operations performed in a previous migration attempt via Spanner migration tool.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Save Session

Spanner Migration Tool provides support for collaborative schema modifications so that users can check-point their schema edits and resume later from the point they left off. It also allows multiple users to work simultaneously on the schema assessment for the same database. For this, users can save a [session](../ui.md/#termsterminology) after any schema modifications by completing a small form wherein they specify the session name, editor name, database name and some notes related to the current session. Once a user clicks on save, an entry is created in the [metadata database](../ui.md/#termsterminology) with corresponding session details and they can resume the session anytime by going to the [Session History](#session-history) section in the home page.

![](https://services.google.com/fh/files/helpcenter/asset-qr6lm8m22fo.png)

## Session History

All the saved [sessions](../ui.md/#termsterminology) show up here with the details about database name, editor name, spanner dialect, etc. Users can resume or download a session from this section. In case a user resumes a session it would be equivalent to the [load session file](../connect-source.md/#load-session-file) connection mechanism, the only difference is that metadata is fetched from the [metadata database](../ui.md/#termsterminology) in the configured spanner instance. In case a user wishes to download a session file, they can do so by clicking on the **Download** button for the required session.

![](https://services.google.com/fh/files/helpcenter/asset-0umdabpdp2e.png)
