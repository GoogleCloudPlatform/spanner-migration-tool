---
layout: default
title: Running Reverse Replication
parent: Reverse Replication
nav_order: 2
---

# Reverse Replication Setup
{: .no_toc }

Spanner migration tool currently does not support reverse replication out-of-the-box.
The launcher.go script can be used instead to setup the resources required for a 
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
The pipeline requires a few GCP resources to be setup. The launcher script creates these resources for you, skipping creation if they already exist. The resources are:
- `Change Stream`: The target spanner database should have a changestream setup with value_capture_type = 'NEW_ROW'. This helps stream CDC events from Spanner.
- `Ordering Dataflow Job`: This dataflow job reads from Spanner CDC, orders the data and pushes it to a PubSub topic.
- `PubSub Topic & Subscriptions`: The topic that the ordering job pushes to needs to be created beforehand. For each shard, a subscription needs to be created, with the subscription name as the corresponding logicalShardId. These names are fetched from the source shards file mentioned later.
- `Writer Dataflow Job`: This reads messages from the PubSub subscriptions, translates them to SQL and writes to the source shards.

## Arguments

The script takes in multiple arguments to orchestrate the pipeline. They are:
- `projectId`: project id of the Spanner instance.
- `dataflowRegion`: region for Dataflow jobs.
- `jobNamePrefix`: job name prefix for the Dataflow jobs, defaults to `reverse-rep`. Automatically converted to lower case due to Dataflow name constraints.
- `changeStreamName`: change stream name to be used. Defaults to `reverseReplicationStream`.
- `instanceId`: spanner instance id.
- `dbName`: spanner database name.
- `metadataInstance`: Spanner instance name to store changestream metadata. Defaults to target spanner instance id.
- `metadataDatabase`: Spanner database name to store changestream metadata, defaults to `change-stream-metadata`.
- `startTimestamp`: timestamp from which the changestream should start reading changes in RFC 3339 format, defaults to empty string which is equivalent to the current timestamp.
- `pubSubDataTopicId`: pub/sub data topic id. DO NOT INCLUDE the prefix 'projects/<project_name>/topics/'. Defaults to 'reverse-replication'.
- `pubSubEndpoint`: Pub/Sub endpoint, defaults to same endpoint as the Dataflow region.
- `sourceShardsFilePath`: GCS file path for file containing shard info. Details on structure mentioned later.
- `sessionFilePath`: GCS file path for session file generated via Spanner migration tool.
- `machineType`: dataflow worker machine type, defaults to n2-standard-4.
- `orderingWorkers`: number of workers for ordering job. Defaults to 5.
- `writerWorkers`: number of workers for writer job. Defaults to 5.

## Pre-requisites
Before running the command, ensure you have the:
1) Target Spanner instance ready
2) Session file already uploaded to GCS
3) Source shards file (more details below) already uploaded to GCS

## Sample sourceShards File
This file contains meta data regarding the source MYSQL shards, which is used to connect to them.
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

{: .note }
The logicalShardId is expected to be a string that begins with a letter, is atleast 3 characters long and and contain only the following characters: letters, numbers, dashes (-), periods (.), underscores (_), tildes (~), percents (%) or plus signs (+). Cannot start with goog.

## Sample Commands
Checkout out the reverse replication folder from the root:
```
cd reverse_replication
```
### Quickstart
Run the launcher command via:
```sh
go run launcher.go -projectId=my-project -dataflowRegion=us-east1 -instanceId=my-instance -dbName=mydb -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json 
``` 
### Custom Names
Run the launcher command via:
```sh
go run launcher.go -projectId=my-project -dataflowRegion=us-east1 -jobNamePrefix=reverse-rep -changeStreamName=mystream -instanceId=my-instance -dbName=mydb -metadataInstance=my-instance -metadataDatabase=stream-metadb -pubSubDataTopicId=my-topic -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json 
``` 
### Tune Dataflow Configs
Run the launcher command via:
```sh
go run launcher.go -projectId=my-project -dataflowRegion=us-east1 -instanceId=my-instance -dbName=mydb -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json -machineType=e2-standard-2 -orderingWorkers=10 -writerWorkers=8
``` 
### Custom PubSub Endpoint
Using a custom regional pubSubEndpoint:
```
go run launcher.go -projectId=my-project -dataflowRegion=us-east1 -instanceId=my-instance -dbName=mydb -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json -pubSubEndpoint=asia-southeast2-pubsub.googleapis.com:443 
```
Using the global pubSubEndpoint:
```
go run launcher.go -projectId=my-project -dataflowRegion=us-east1 -instanceId=my-instance -dbName=mydb -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json -pubSubEndpoint=pubsub.googleapis.com:443
```
