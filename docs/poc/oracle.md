---
layout: default
title: Oracle
parent: POC migrations
nav_order: 4
---

# POC migrations for Oracle
{: .no_toc }

Spanner migration tool can connect to an OracleDB is directly. We currently do not support dump file mode for Oracle.

Note that _'oracle'_ can be used as an identifier with the flag `-source` in the command line.

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

## Directly connecting to an Oracle database

In this case, Spanner migration tool connects directly to the Oracle database to
retrieve table schema and data. Set the `-source=oracle` and corresponding
source profile connection parameters `host`, `port`, `user`, `dbName` and
`password`.

For example, to perform schema conversion, run

```sh
spanner-migration-tool schema -source=oracle -source-profile="host=<>,port=<>,user=<>,dbName=<>,password=<>"
```

In Oracle DB, USER is the account name, SCHEMA is the set of objects owned by that user. Oracle creates the SCHEMA object as part of the CREATE USER statement and the SCHEMA has the same name as the USER.

`dbName` will be the SID of the Database used. The Oracle System ID (SID) is used to uniquely identify a particular database on a system.
