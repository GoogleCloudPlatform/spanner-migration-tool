---
layout: default
title: CLI flags
parent: SMT CLI
nav_order: 5
---

# CLI Flags
{: .no_toc }

Below is the description of the configuration parameters can be passed to the Spanner migration tool CLI flags.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Source Profile

Spanner migration tool accepts the following params for --source-profile,
specified as "key1=value1,key2=value,..." pairs:

* **`file`**: Specifies the full path of the file to use for reading source database
schema and/or data. This param is optional, and the file can also be piped to
stdin, if available locally. If the file is located in Google Cloud Storage (GCS), you can use the
following format: `file=gs://{bucket_name}/{path/to/file}`. Please ensure you
have read pemissions to the GCS bucket you would like to use.

* **`format`**: Specifies the format of the file. Supported file formats are `dump` and `csv`. This param is also optional, and
defaults to `dump`. This may be extended in future to support other formats
such as `avro` etc.

* **`host`**: Specifies the host name for the source database.

* **`user`**: Specifies the user for the source database.

* **`dbName`**: Specifies the name of the source database. For Cassandra, this corresponds to the keyspace.

* **`port`**: Specifies the port for the source database.

* **`password`**: Specifies the password for the source database.

* **`datacenter`**: Optional flag. Specifies the datacenter for the source database. This parameter is specific to Cassandra source and will be ignored for all other databases.

* **`streamingCfg`**: Optional flag. Specifies the file path for streaming config.
Please note that streaming migration is only supported for MySQL and PostgreSQL databases currently.
Here is an example of a [streamingCfg JSON](./config-json.md#streamingcfg-for-non-sharded-minimal-downtime-migrations) and [how to use it in the CLI](./schema-and-data.md#examples).

## Target Profile

Spanner migration tool accepts the following options for --target-profile,
specified as "key1=value1,key2=value,..." pairs:

* **`project`**: Specifies the name of the Google Cloud Project in which the Spanner instance is present. If the project is not specified, Spanner migration tool will try to fetch the configured project in the gCloud CLI.

{: .note }
This project flag can have different value than the --project flag in the main command. In some cases, you may want to keep the spanner instance in a separate GCP project than the project where all the migration resources are created. This project flag refers to the project in which the Spanner instance is present and --project flag in the main command refers to the project where the tool can create resources (Dataflow jobs, GCS Buckets etc.) for the migration.

* **`dbName`**: Specifies the name of the Spanner database to create. This must be a
new database. If dbName is not specified, Spanner migration tool creates a new unique
dbName.

* **`instance`**: Specifies the Spanner instance to use. The new database will be
created in this instance. If not specified, the tool automatically determines an
appropriate instance using gcloud.

* **`dialect`**: Specifies the dialect of Spanner database. By default, Spanner
databases are created with GoogleSQL dialect. You can override the same by
setting `dialect=PostgreSQL` in the `-target-profile`. Learn more about support
for PostgreSQL dialect in Cloud Spanner [here](https://cloud.google.com/spanner/docs/postgresql-interface).