# Reverse Replication Setup

HarbourBridge currently does not support reverse replication out-of-the-box.
The launcher.go script can be used instead to setup the resources required for a 
reverse replication pipeline.

## Resources
The pipeline requires a few GCP resources to be setup. The launcher script creates these resources for you, skipping creation if they already exist. The resources are:
- `Change Stream`: The target spanner database should have a changestream setup with value_capture_type = 'NEW_ROW'. This helps stream CDC events from Spanner.
- `Ordering Dataflow Job`: This dataflow job reads from Spanner CDC, orders the data and pushes it to a PubSub topic.
- `PubSub Topic & Subscriptions`: The topic that the ordering job pushes to needs to be created beforehand. For each shard, a subscription needs to be created, with the subscription name as the corresponding logicalShardId. These names are fetched from the source shards file mentioned later.
- `Writer Dataflow Job`: This reads messages from the PubSub subscriptions, translates them to SQL and writes to the source shards.

This launcher script will create

## Arguments

The script takes in multiple arguments to orchestrate the pipeline. They are:
- `projectId`: project id of the Spanner instance.
- `dataflowRegion`: region for Dataflow jobs.
- `jobNamePrefix`: job name prefix for the Dataflow jobs.
- `changeStreamName`: change stream name.
- `instanceId`: spanner instance id.
- `dbName`: spanner database name.
- `metadataInstance`: Spanner instance name to store changestream metadata.
- `metadataDatabase`: Spanner database name to store changestream metadata.
- `pubSubDataTopicId`: Pub/Sub data topic id. Should be of the form projects/my-project/topics/my-topic.
- `pubSubEndpoint`: Pub/Sub endpoint, defaults to same endpoint as the Dataflow region.
- `sourceShardsFilePath`: GCS file path for file containing shard info. Details on structure mentioned later.
- `sessionFilePath`: GCS file path for session file generated via HarbourBridge.

## Sample Command

```sh
go run launcher.go -projectId=my-project -dataflowRegion=us-east1 -jobNamePrefix=reverse-rep -changeStreamName=mystream -instanceId=my-instance -dbName=mydb -metadataInstance=my-instance -metadataDatabase=stream-metadb -pubSubDataTopicId=projects/my-project/topics/my-topic -sourceShardsFilePath=gs://bucket-name/shards.json  -sessionFilePath=gs://bucket-name/session.json  
``` 

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
