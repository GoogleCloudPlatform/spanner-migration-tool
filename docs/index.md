---
layout: default
title: Home
nav_order: 1
description: "Spanner migration tool is a stand-alone open source tool for Cloud Spanner evaluation and migration."
permalink: /
---

# Spanner migration tool

{: .fs-9 }

Spanner migration tool (SMT) is a stand-alone open source tool for Cloud Spanner evaluation and migration.
{: .fs-6 .fw-300 }

[Learn More](#what-does-spanner-migration-tool-do){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View it on GitHub][SMT repo]{: .btn .fs-5 .mb-4 .mb-md-0 }

---

Spanner migration tool is a stand-alone open source tool for Cloud Spanner evaluation and
migration, using data from an existing PostgreSQL, MySQL, SQL Server, Oracle or DynamoDB database.
The tool ingests schema and data from either a pg_dump/mysqldump file or directly
from the source database, and supports both schema and data migration.

## What does Spanner migration tool do?

### Schema migrations

Spanner migration tool is designed to simplify Spanner evaluation and migration.
Certain features of relational databases, especially those that don't
map directly to Spanner features, are ignored, e.g. stored functions and
procedures, and sequences. Types such as integers, floats, char/text, bools,
timestamps, and (some) array types, map fairly directly to Spanner, but many
other types do not and instead are mapped to Spanner's `STRING(MAX)`. Spanner migration tool automatically builds a Spanner schema from the schema of the source database. This schema can be customized using the Spanner migration tool schema assistant and a new Spanner database is created using the Spanner schema built.
View Spanner migration tool as a way to get up and running fast, so you can focus on
critical things like tuning performance and getting the most out of
Spanner. Expect that you'll need to tweak and enhance what Spanner migration tool
produces.

### Data migrations

Spanner migration tool supports both small-scale on-prem POC migrations as well as production grade minimal downtime migrations using GCP services (Datastream and Dataflow) -

* Minimal Downtime migration - A minimal downtime migration consists of two components, migration of existing data from the database and the stream of changes (writes and updates) that are made to the source database during migration, referred to as change database capture (CDC). Using Spanner migration tool, the entire process where Datastream reads data from the source database and writes to a GCS bucket and data flow reads data from GCS bucket and writes to spanner database can be orchestrated using a unified interface. Performing schema changes on the source database during the migration is not supported. This is the suggested mode of migration for most databases.

* POC Migration -  Spanner migration tool reads data from source database and writes it to the database created in Cloud Spanner. Changes which happen to the source database during the POC migration may or may not be written to Spanner. To achieve consistent version of data, stop writes on the source while migration is in progress, or use a read replica. Performing schema changes on the source database during the migration is not supported. While there is no technical limit on the size of the database, it is recommended for migrating moderate-size datasets to Spanner(up to about 100GB).

[SMT repo]: https://github.com/GoogleCloudPlatform/spanner-migration-tool

## About the project

### Contributing

Spanner migration tool is an open-source project and we'd love to accept contributions to it. Details on how to contribute are listed [here](./contributing.md).

## License

Spanner Migration tool is licensed during the [Apache 2.0 License](https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/LICENSE)

{: .note }
Spanner migration tool is an officially supported Google product. Please reach out to [GCP support](https://support.google.com/cloud/answer/6282346?hl=en) to get help.
