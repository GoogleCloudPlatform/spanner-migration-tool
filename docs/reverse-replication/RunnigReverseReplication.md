---
layout: default
title: Running Reverse Replication
parent: Reverse Replication
nav_order: 2
---

# Reverse Replication Setup
{: .no_toc }

Spanner migration tool currently does not support reverse replication out-of-the-box.
The run_reverse_replication.go script can be used instead to setup the resources required for a 
reverse replication pipeline.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Resources
The pipeline requires a few GCP resources to be setup. The runner script creates these resources for you, skipping creation if they already exist. The resources are:
- `Change Stream`: The target spanner database should have a changestream setup with value_capture_type = 'NEW_ROW'. This helps stream CDC events from Spanner.
- `Reader Dataflow Job`: This dataflow job reads from Spanner CDC, and writes them to GCS.
- `Writer Dataflow Job`: This reads the GCS files, orders the records, translates them to SQL and writes to the source shards.

## Arguments

The script takes in multiple arguments to orchestrate the pipeline. They are:
- `projectId`: Project id of the Spanner instance.
- `dataflowRegion`: Region for Dataflow jobs.
- `jobNamePrefix`: Job name prefix for the Dataflow jobs, defaults to `reverse-rep`. Automatically converted to lower case due to Dataflow name constraints.
- `changeStreamName`: Change stream name to be used. Defaults to `reverseReplicationStream`.
- `instanceId`: Spanner instance id.
- `dbName`: Spanner database name.
- `metadataInstance`: Spanner instance name to store changestream metadata. Defaults to target spanner instance id.
- `metadataDatabase`: Spanner database name to store changestream metadata, defaults to `change_stream_metadata`.
- `startTimestamp`: Timestamp from which the changestream should start reading changes in RFC 3339 format, defaults to empty string which is equivalent to the current timestamp.
- `windowDuration`: The window duration/size in which change stream data will be written to Cloud Storage. Defaults to 10 seconds.
- `gcsPath`: The GCS directory where the change stream data resides.Default is gs://reverse-replication/buffer.
- `filtrationMode`: The flag to decide whether or not to filter the forward migrated data.Defaults to forward_migration.
- `metadataTableSuffix`: The suffix to apply when creating metadata tables.Helpful in case of multiple runs.Default is no suffix.
- `readerSkipDirectoryName`: Records skipped from reverse replication are written to this directory. Defaults to: skip.
- `sourceShardsFilePath`: GCS file path for file containing shard info. Details on structure mentioned later.
- `sessionFilePath`: GCS file path for session file generated via Spanner migration tool.
- `sourceDbTimezoneOffset`: The timezone offset with respect to UTC for the source database.Defaults to +00:00.
- `writerRunMode`: Whether the writer to source job runs in regular or reprocess mode. Default is regular.
- `machineType`: Dataflow worker machine type, defaults to n2-standard-4.
- `readerWorkers`: Number of workers for ordering job. Defaults to 5.
- `writerWorkers`: Number of workers for writer job. Defaults to 5.
- `vpcNetwork`: Name of the VPC network to be used for the dataflow jobs
- `vpcSubnetwork`: Name of the VPC subnetwork to be used for the dataflow jobs. Subnet should exist in the same region as the 'dataflowRegion' parameter.
- `vpcHostProjectId`: Project ID hosting the subnetwork. If unspecified, the 'projectId' parameter value will be used for subnetwork..
- `serviceAccountEmail`: The email address of the service account to run the job as.
- `networkTags`: Network tags addded to the Dataflow jobs worker and launcher VMs.
- `readerWorkers`: Number of workers for Spanner reader job.
- `writerWorkers`: Number of workers for Source writer job.
- `spannerReaderTemplateLocation`: The dataflow template location for the Spanner reader job, defaults to gs://dataflow-templates/2023-10-31-00_RC00/flex/Spanner_Change_Streams_to_Sharded_File_Sink.
- `sourceWriterTemplateLocation`: The dataflow template location for the Source writer job, defaults to gs://dataflow-templates/2023-10-31-00_RC00/flex/GCS_to_Sourcedb
- `jobsToLaunch`: Whether to launch the spanner reader job or the source writer job or both. Default is both. Support values are both,reader,writer.
- `skipChangeStreamCreation`: Whether to skip the change stream creation. Default is false.
- `skipMetadataDatabaseCreation`: Whether to skip Metadata database creation. Default is false.

## Pre-requisites
Before running the command, ensure you have the:
1) Target Spanner instance ready
2) Session file already uploaded to GCS
3) Source shards file (more details below) already uploaded to GCS

## Sample sourceShards File
This file contains meta data regarding the source MYSQL shards, which is used to connect to them. This should be present even if there is a single source database shard.
The file should be a list of JSONs as:
```
[
    {
    "logicalShardId": "shard1",
    "host": "10.11.12.13",
    "user": "root",
    "password": "mypwd",
    "port": "3306",
    "dbName": "db1"
    },
    {
    "logicalShardId": "shard2",
    "host": "10.11.12.14",
    "user": "root",
    "password": "mypwd",
    "port": "3306",
    "dbName": "db2"
    }
]
```

## Sample Commands
Checkout out the reverse replication folder from the root:
```
cd reverse_replication
```
### Quickstart
Run the launcher command via:
```sh
go run run_reverse_replication.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<instance> -dbName=<database> -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json -startTimestamp=<date in format like 2023-11-02T09:37:00Z> 
``` 
### Custom Names
Run the launcher command via:
```sh
go run run_reverse_replication.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<instance> -dbName=<database> -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json -startTimestamp=<date in format like 2023-11-02T09:37:00Z> -jobNamePrefix=reverse-rep
``` 
### Tune Dataflow Configs
Run the launcher command via:
```sh
go run run_reverse_replication.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<instance> -dbName=<database> -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json -startTimestamp=<date in format like 2023-11-02T09:37:00Z>  -machineType=e2-standard-2 -readerWorkers=10 -writerWorkers=8
``` 
