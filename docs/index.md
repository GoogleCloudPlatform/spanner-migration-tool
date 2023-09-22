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

Spanner migration tool is designed to simplify Spanner evaluation and migration. It automatically builds a Spanner schema from the schema of the source database. This schema can be customized using the web based schema assistant UI. Expect that you'll need to tweak and enhance what Spanner migration tool produces.

### Data migrations

Spanner migration tool supports production grade minimal downtime migrations using GCP services (**Datastream and Dataflow**).
It can also be used to do small scale on-prem POC migrations to get a feel of Spanner.

- **Minimal Downtime migration** - This is the production ready, recommended mode of migration for most databases. It provides a unified interface to configure an end-to-end pipeline to transfer both existing and new data from source database to Spanner. More details about minimal downtime migrations are [here](./minimal/minimal.md).

- **POC Migration** -  This mode is useful to get up and running quickly to get a feel of what migrating to Spanner would look like. This mode of migration uses the local machine's resources (on which SMT is running) to write data to Spanner. This is an offline migration for migrating moderate-size datasets to Spanner(up to about 100GB). More details about POC migrations are [here](./poc/poc.md).

### Reverse Replication

To launch reverse replication, refer details [here](./reverse-replication/ReverseReplication.md).

## Supported Sources and Targets

- **Schema Migrations**: SMT supports schema migrations for MySQL, PostgreSQL, SQLServer and Oracle.
- **Data Migrations**: SMT supports minimal downtime migrations for MySQL, PostgreSQL and Oracle, and POC migration for MySQL, PostgreSQL, SQLServer and Oracle.

## About the project

### Contributing

Spanner migration tool is an open-source project and we'd love to accept contributions to it. Details on how to contribute are listed [here](./contributing.md).

### License

Spanner Migration tool is licensed during the [Apache 2.0 License](https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/LICENSE).

{: .note }
Spanner migration tool is an officially supported Google product. Please reach out to [GCP support](https://support.google.com/cloud/answer/6282346?hl=en) to get help.

[SMT repo]: https://github.com/GoogleCloudPlatform/spanner-migration-tool