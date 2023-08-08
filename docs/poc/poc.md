---
layout: default
title: POC migrations
nav_order: 8
has_children: true
permalink: /poc
---

# POC migrations
{: .no_toc }

Spanner migration tool can two POC migrations in two ways:

## Dump Files

{: .highlight }
Dump files are only supported for MySQL and PostgreSQL.

Spanner migration tool can accept MySQL and PostgreSQL dump files, parse them, generate a Spanner compatible schema for it and then migrate the `schema`/`data`/`schema-and-data` to the Spanner database.

## Connecting to source database

Spanner migration tool (SMT) reads data from source database and writes it to the database created in Cloud Spanner. Changes which happen to the source database during the POC migration may or may not be written to Spanner. To achieve consistent version of data, stop writes on the source while migration is in progress, or use a read replica. Performing schema changes on the source database during the migration is not supported. While there is no technical limit on the size of the database, it is recommended for migrating moderate-size datasets to Spanner(up to about 100GB).
