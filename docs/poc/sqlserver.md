---
layout: default
title: SQL Server
parent: POC migrations
nav_order: 5
---

# POC migrations for Oracle
{: .no_toc }

Spanner migration tool can be run directly on a SQL Server database (via go's database/sql package).

{: .highlight }
Following instructions assume you have setup SMT by following the instructions in the [installation](../install.md) guide.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Directly connecting to a SQL Server database

In this case, Spanner migration tool connects directly to the SQL Server database to
retrieve table schema and data. Set the `-source=sqlserver` and corresponding
source profile connection parameters `host`, `port`, `user`, `dbName` and
`password`.

For example, to perform schema conversion, run

```sh
spanner-migration-tool schema -source=sqlserver -source-profile="host=<>,port=<>,user=<>,dbName=<>"
```

Parameters `port` and `password` are optional. Port (`port`) defaults to `1433`
for SQL Server source. Password can be provided at the password prompt.
