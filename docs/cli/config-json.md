---
layout: default
title: CLI Configuration JSONs
parent: SMT CLI
nav_order: 6
---

# Configuration JSONs
{: .no_toc }

The SMT CLI expects configuration JSONs when running minimal downtime migrations in two key modes:

- **Non-Sharded Minimal Downtime Migrations:** In this mode, the SMT CLI expects a `streamingCfg` parameter containing the configuration details in JSON format.

- **Sharded Minimal Downtime Migrations:** In this mode, the SMT CLI also expects a `config` parameter containing the configuration details in JSON format similar to `streamingCfg`, but caters to sharded deployments.


<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## StreamingCfg for Non-Sharded Minimal Downtime Migrations
This json is passed to the `streamingCfg` parameter via the `--source-profile` flag when running non-sharded minimal downtime migrations.

{: .highlight}
The empty fields are optional.

```json
{
    "datastreamCfg": {
        "streamId": "",
        "streamLocation": "us-central1",
        "streamDisplayName": "",
        "sourceConnectionConfig": {
            "Name": "my-source-connection-profile",
            "Location": "us-central1"
        },
        "destinationConnectionConfig": {
            "Name": "my-destination-connection-profile",
            "Location": "us-central1",
            "Prefix": ""
        },
        "properties": "replicationSlot=slot_name,publication=pub_name", 
        "tableList": ["table1", "table2", "table3"],
        "maxConcurrentBackfillTasks": "50",
        "maxConcurrentCdcTasks": "5"
    },
    "gcsCfg": {
        "ttlInDaysSet": true,
        "ttlInDays": 8
    },
    "dataflowCfg": {
        "projectId": "my-project",
        "jobName": "",
        "location": "us-central1",
        "hostProjectId": "my-vpc-host-project-id",
        "network": "my-vpc-network",
        "subnetwork": "my-vpc-subnetwork",
        "maxWorkers": "50",
        "numWorkers": "1",
        "machineType": "n1-standard-2",
        "serviceAccountEmail": "",
        "additionalUserLabels": "",
        "kmsKeyName": "",
        "gcsTemplatePath": "",
    },
    "tmpDir": "gs://my-bucket/path/to/directory",
}
```

{: .note}
- `datastreamCfg.properties` is specific to postgres, used to specify replication slot and publication name.
- `datastreamCfg.tmpDir` is used to store SMT metadata files.


## Config for Sharded Minimal Downtime Migrations

This json is passed to the `config` parameter via the `--source-profile` flag when running a sharded minimal downtime migration.


{: .highlight}
The empty fields are optional.


```json
{
    "configType": "dataflow",
    "shardConfigurationDataflow": {
        "schemaSource": {
            "host": "127.0.0.1",
            "user": "root",
            "password": "mypass",
            "port": "3306",
            "dbName": "test"
        },
        "dataShards": [
            {
                "dataShardId": "smt_datashard_Jo1B_gVrJ",
                "srcConnectionProfile": {
                    "name": "test-src-conn",
                    "location": "us-central1"
                },
                "dstConnectionProfile": {
                    "name": "test-dst-conn",
                    "location": "us-central1"
                },
                "tmpDir": "gs://my-bucket/path-to-folder",
                "streamLocation": "us-central1",
                "databases": [
                    {
                        "dbName": "test",
                        "databaseId": "logical_shard1",
                        "refDataShardId": "smt_datashard_Jo1B_gVrJ"
                    }
                ]
            }
        ],
        "datastreamConfig": {
            "maxConcurrentBackfillTasks": "50",
            "maxConcurrentCdcTasks": "5"
        },
        "gcsConfig": {
            "ttlInDaysSet": true,
            "ttlInDays": "1"
        },
        "dataflowConfig": {
            "projectId": "my-project",
            "jobName": "",
            "location": "us-central1",
            "hostProjectId": "my-vpc-host-project",
            "network": "my-vpc-network",
            "subnetwork": "my-vpc-subnetwork",
            "maxWorkers": "50",
            "numWorkers": "1",
            "machineType": "n1-standard-2",
            "serviceAccountEmail": "",
            "additionalUserLabels": "",
            "kmsKeyName": "",
            "gcsTemplatePath": ""
        }
    }
}
```
