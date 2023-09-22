---
layout: default
title: POC migration
parent: Troubleshooting
nav_order: 2
---

# Error handling
{: .no_toc }

The Spanner migration tool's POC migration can fail for a number of reasons.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## No space left on device

Spanner migration tool needs to read the pg_dump/mysqldump output twice, once to build
a schema and once for data ingestion. When pg_dump/mysqldump output is directly
piped to Spanner migration tool, `stdin` is not seekable, and so we write the output to
a temporary file. That temporary file is created via Go's ioutil.TempFile.
On many systems, this creates a file in `/tmp`, which is sometimes configured
with minimal space. A simple workaround is to separately run pg_dump/mysqldump
and write its output to a file in a directory with sufficient space. For example,
if the current working directory has space, then:

```sh
{ pg_dump/mysqldump } > tmpfile
spanner-migration-tool < tmpfile
```

Make sure you cleanup the tmpfile after Spanner migration tool has been run. Another
option is to set the location of Go's TempFile e.g. by setting the `TMPDIR`
environment variable.

## Unparsable dump output

Spanner migration tool uses the [pg_query_go](https://github.com/pganalyze/pg_query_go)
library for parsing pg_dump and [pingcap parser](https://github.com/pingcap/parser)
for parsing mysqldump. It is possible that the pg_dump/mysqldump output is
corrupted or uses features that aren't parseable. Parsing errors should
generate an error message of the form `Error parsing last 54321 line(s) of input`.

## Credentials problems

Spanner migration tool uses standard Google Cloud credential mechanisms for accessing
Cloud Spanner. If this is mis-configured, you may see errors containing
"unauthenticated", or "cannot fetch token", or "could not find default
credentials". You might need to run `gcloud auth application-default login`.
See the [Before you begin](#before-you-begin) section for details.

## Can't create database

In this case, the error message printed by the tool should help identify the
cause. It could be an API permissions issue. For example, the Cloud Spanner API
may not be appropriately configured. See [Before you begin](#before-you-begin)
section for details. Alternatively, you have have hit the limit on the number of
databases per instances (currently 100). This can occur if you re-run the
Spanner migration tool tool many times, since each run creates a new database. In this
case you'll need to [delete some
databases](https://cloud.google.com/spanner/docs/getting-started/go/#delete_the_database).

## Database-Specific Issues

The schema, report, and bad-data files [generated](../reports.md/#file-descriptions) contain detailed information
about the schema and data conversion process, including issues and problems
encountered.
