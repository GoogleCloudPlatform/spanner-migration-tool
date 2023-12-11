---
layout: default
title: Known Limitations
nav_order: 12
permalink: /known-limitations
---

# Known Limitations
{: .no_toc }

Please refer to the [issues section](https://github.com/GoogleCloudPlatform/spanner-migration-tool/issues)
 on Github for a full list of known issues.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

### Schema Conversion

- Loading dump files from SQL Server, Oracle and DynamoDB is not supported
- Schema Only Mode does not create foreign keys
- Foreign key actions such as [ON DELETE CASCADE](https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language#create_table) are not supported. If you do not specify a foreign key action, Spanner infers NO ACTION as the default action
- Migration of check constraints, functions and views is not supported
- Schema recommendations are based on static analysis of the schema only
- PG Spanner dialect support is limited, and is not currently available on the UI

### Minimal Downtime Data Migrations

- Minimal downtime migrations for SQL Server and DynamoDB are not supported
- Requires a direct connection to the database to run and hence will not be
 available while reading from Dump files.
- Expected downtime will be in the order of a few minutes while the pipeline gets
 flushed.
- This flow depends on Datastream, and all the [constraints of Datastream](https://cloud.google.com/datastream/docs/faq#behavior-and-limitations)
 apply to these migrations
- Migration from sharded databases is not natively supported
- Edits to primary keys and unique indexes are supported, but the user will 
need to ensure that the new primary key/unique indexes retain uniqueness in
the data. This is not verified during updation of the keys
- When the Spanner table PKs are different from the source keys, updates on the spanner PK columns can potentially lead to data inconsistencies. The updates can be potentially treated as a new insert or update some different row
- Interleaved rows and rows with foreign key constraints are retried 500 times.
 Exhaustion of retries results in these rows being pushed into a dead letter queue.
- Conversion to Spanner ARRAY type is currently not supported
- MySQL types BIT and TIME are not converted correctly
- PostgreSQL types bit, bit varying, bytea and time not converted correctly.

### Access Control

- Spanner migration tool does not support database roles and privileges. If users wish to use Spanner [fine-grained access control](https://cloud.google.com/spanner/docs/configure-fgac) as an IAM principal, then they can manually create database roles, grant the appropriate memberships and privileges to these roles, and grant access to database roles to the IAM principal. Alternatively, users can grant [database-level access](https://cloud.google.com/spanner/docs/grant-permissions#database-level_permissions) to an IAM principal.
