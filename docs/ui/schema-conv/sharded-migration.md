---
layout: default
title: Sharded migration schema
parent: Schema Conversion Workspace
grand_parent: SMT UI
nav_order: 9
---

# Sharded migration schema changes
{: .no_toc }

When a sharded migration is selected by the user in the [connect to database](../connect-source.md#connect-to-database) page, SMT automatically makes some changes to the converted Spanner schema for performing sharded migrations.

{: .note }
Sharded migrations are only supported for `MySQL` currently.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Addition of `migration_shard_id`

SMT adds a new column - `migration_shard_id` to each table of the source database. Each row getting copied to Spanner can be mapped back to its source. This has several advantages:

1. Enables [reverse replication](../../reverse-replication/ReverseReplicationUserGuide.md) since each row in Spanner can be mapped back to its own source shard.
2. Enabling restarts and failure handling. For example, adding a `migration_shard_id` enables usage of [partitioned DML](https://cloud.google.com/spanner/docs/dml-partitioned) to delete all the data migrated for a shard and restart a migration.

{: .important }
The `migration_shard_id` column is populated via the database to shardId map that is configured during connection profile configuration in the [prepare migration page](../prepare-migration/prepare.md). For details on this configuration, refer [here](../prepare-migration/conn-profiles.md#form-based-configuration).

![](https://services.google.com/fh/files/misc/shard_schema.png)

## `migration_shard_id` as Primary Key
It is a common practice to use auto-incremented primary keys in many source databases. However during a sharded migration, where
multiple source shards are merged into a single Spanner Database, this may lead to conflicts during the data migration. To avoid
these conflicts, the `migration_shard_id` is added to the primary key of each table by default. 

![](https://services.google.com/fh/gumdrop/preview/misc/hb_shard_id_rule_view.png)

To disable this behaviour, please click on "View Rule" and delete the shard_id rule as shown below.

![](https://services.google.com/fh/gumdrop/preview/misc/hb_delete_shard_id_rule.png)


