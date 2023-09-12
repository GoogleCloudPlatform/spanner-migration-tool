# Reverse Replication Setup

Spanner migration tool currently does not support reverse replication out-of-the-box.
The launcher.go script can be used instead to setup the resources required for a 
reverse replication pipeline.

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

# Reverse Replication Limitations
## Infinite Loop Scenario under high Spanner throughput
The Dataflow job that reads change streams and writes to PubSub can get stuck in an infinite loop retrying the same set of records in certain scenarios. These scenarios can occur when there are a lot of changestream records to be read in a short interval of time, which can happen in the following situations:
- There is an unexpected spike in Spanner activity.
- The pipeline is started with a date in the past (due to issues that required downtime, such as a bug fix).
Currently, there is no way to revert this within the same job. More details and recovery steps are below.

### Recovery steps for the infinite loop
1. The user must track the last handled window that was successfully processed - this can be obtained from the Dataflow logs or by checking the watermark of the job stage.
2. Specify a different partition metadata database or drop the previous metadata tables - since watermarks for a given partition are persisted and hence will be read from that point onwards. So the pipeline must begin with a clean partition metadata database.Note that the metadata tables can be dropped by giving DROP TABLE\<table name\> statements in the Cloud UI.
3. The current pipeline must be updated - the user must specify the start time of the change stream query as the last successfully processed timestamp and the end time of the change stream as the time that would result in ~1GB writes. Example, if the average record size is 1KB, then 10,00,000 records should be processed in a window and if the TPS during that window was 20K, then the window must end at 50 seconds. Detailed steps to update the Dataflow job are given [here](https://cloud.google.com/dataflow/docs/guides/updating-a-pipeline#gcloud-cli).
4. Once this window is processed, the pipeline must be updated with the next set of start and end times, until the pipeline catches up and finally the end timestamp need not be passed.

## Throughput
- The maximum stable throughput is around 1200 Writes per Second per shard. Beyond this, PubSub starts accumulating a backlog.

## Security
- The pipeline expects the sourceShardsFile which contains the connection info for the MySQL shards including the passwords. This file is expected to be stored on GCS.

## Reverse Transformations
Reverse transformation can not be supported for following scenarios out of the box:
1. The table in original database has single column while in Spanner it’s split into multiple - example POINT to X,Y coordinates
2. Adding column in Spanner that does not exist in source - in this case the column cannot be replicated
3. Deleting column in Spanner that is mandatory in source
4. Spanner PK is UUID while source PK is auto-increment key 
5. Spanner table has more columns as part of PK than the source 
6. Spanner column has a datatype not supported via the Spanner Migration Tool schema conversion. 
7. CLOB will not be read from GCS and put in source 
8. DELETES on Spanner that have Primary key columns different from the Source database column - such records will be dropped
9. Primary key of the source table cannot be determined - such records will be dropped
10. STRING types with maxlength less than the value (Eg: STRING(20) on Spanner -> varchar(16) on MySQL) being replicated will throw an error. It will not auto-truncate during replication.
11. SPATIAL data types are not supported.
In the above cases, custom code will need to be written to perform reverse transformation. The source code can be taken from [here](https://github.com/aksharauke/DataflowTemplates/tree/main/v2/ordered-changestream-buffer-to-sourcedb) and extended to write these custom transforms.
