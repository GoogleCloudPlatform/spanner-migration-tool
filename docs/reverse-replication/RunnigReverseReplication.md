---
layout: default
title: Running Reverse Replication
parent: Reverse Replication
nav_order: 2
---

# Reverse Replication Setup
{: .no_toc }

Spanner migration tool currently does not support reverse replication out-of-the-box.
The reverse_replication_runner.go script can be used instead to setup the resources required for a 
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
- `Metadata Database` : This is the metadata database that holds the pipeline related metadata.
- `Reader Dataflow Job`: This dataflow job reads from Spanner CDC, and writes them to GCS.
- `Writer Dataflow Job`: This reads the GCS files, orders the records, translates them to SQL and writes to the source shards.

## Arguments

The script takes in multiple arguments to orchestrate the pipeline. They are:

- `changeStreamName`: Change stream name to be used. Defaults to `reverseReplicationStream`.
- `dataflowRegion`: Region for Dataflow jobs.
- `dbName`: Spanner database name.
- `filtrationMode`: The flag to decide whether or not to filter the forward migrated data.Defaults to forward_migration.
- `gcsPath`: The GCS directory where the change stream data resides.The GCS directory should be pre-created.
- `instanceId`: Spanner instance id.
- `jobNamePrefix`: Job name prefix for the Dataflow jobs, defaults to `smt-reverse-replication`. Automatically converted to lower case due to Dataflow name constraints.
- `jobsToLaunch`: whether to launch the spanner reader job or the source writer job or both. Default is both. Support values are both,reader,writer.
- `machineType`: dataflow worker machine type, defaults to n2-standard-4.
- `metadataDatabase`: Spanner database name to store changestream metadata, defaults to `rev_repl_metadata`.
- `metadataInstance`: Spanner instance name to store changestream metadata. Defaults to target spanner instance id.
- `metadataTableSuffix`: The suffix to apply when creating metadata tables.Helpful in case of multiple runs.Default is no suffix.
- `networkTags`: network tags addded to the Dataflow jobs worker and launcher VMs.
- `projectId`: Project id of the Spanner instance.
- `sessionFilePath`: GCS file path for session file generated via Spanner migration tool.
- `serviceAccountEmail`: the email address of the service account to run the job as.
- `skipChangeStreamCreation`: whether to skip the change stream creation. Default is false.
- `skipMetadataDatabaseCreation`: whether to skip Metadata database creation.Default is false.
- `sourceDbTimezoneOffset`: the timezone offset with respect to UTC for the source database.Defaults to +00:00.
- `sourceShardsFilePath`: GCS file path for file containing shard info. Details on structure mentioned later.
- `sourceWriterTemplateLocation` : the dataflow template location for the Source writer job.
- `spannerReaderTemplateLocation`: the dataflow template location for the Spanner reader job
- `startTimestamp`: Timestamp from which the changestream should start reading changes in RFC 3339 format, defaults to empty string which is equivalent to the current timestamp.
- `readerShardingCustomClassName`: the fully qualified custom class name for sharding logic.
- `readerShardingCustomJarPath` : the GCS path to custom jar for sharding logic.
- `readerSkipDirectoryName`: Records skipped from reverse replication are written to this directory. Defaults to: skip.
- `readerRunMode`: whether the reader from Spanner job runs in regular or resume mode. Default is regular.
- `readerWorkers`: number of workers for ordering job. Defaults to 5.
- `windowDuration`: The window duration/size in which change stream data will be written to Cloud Storage. Defaults to 10 seconds.
- `writerRunMode`: whether the writer to source job runs in regular,reprocess,resumeFailed,resumeSuccess or resumeAll mode. Default is regular.
- `writerWorkers`: number of workers for writer job. Defaults to 5.
- `vpcHostProjectId`: project ID hosting the subnetwork. If unspecified, the 'projectId' parameter value will be used for subnetwork.
- `vpcNetwork`: name of the VPC network to be used for the dataflow jobs
- `vpcSubnetwork`: name of the VPC subnetwork to be used for the dataflow jobs. Subnet should exist in the same region as the 'dataflowRegion' parameter.


## Pre-requisites
Before running the command, ensure you have the:
1) Target Spanner instance ready
2) Session file already uploaded to GCS
3) Source shards file (more details below) already uploaded to GCS
4) GCS path for buffering the data exists

## Sample sourceShards File
This file contains meta data regarding the source MYSQL shards, which is used to connect to them. This should be present even if there is a single source database shard.
The database user password should be kept in [Secret Manager](#https://cloud.google.com/security/products/secret-manager) and it's URI needs to be specified in the file.
The file should be a list of JSONs as:
```
[
    {
    "logicalShardId": "shard1",
    "host": "10.11.12.13",
    "user": "root",
    "secretManagerUri":"projects/123/secrets/rev-cmek-cred-shard1/versions/latest",
    "port": "3306",
    "dbName": "db1"
    },
    {
    "logicalShardId": "shard2",
    "host": "10.11.12.14",
    "user": "root",
    "secretManagerUri":"projects/123/secrets/rev-cmek-cred-shard2/versions/latest",
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
### Quickstart reverse replication with all defaults

```
go run reverse-replication-runner.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<spanner-instance> -dbName=<spanner-database> -sourceShardsFilePath=gs://bucket-name/shards.json -sessionFilePath=gs://bucket-name/session.json -gcsPath=gs://bucket-name/<directory>
``` 

The response looks something like this.
The Dataflow job ids can be captured to trigger manual shutdown.
Note the defaults used.
The gcloud command can be saved to retrigger a specific job if needed.

```
Setting up reverse replication pipeline...
metadataInstance not provided, defaulting to target spanner instance id:  <spanner instance id>
changestream <change stream name> not found
Creating changestream
Successfully created changestream <change stream name>
Created metadata db projects/<project-name>/instances/<instance-name>/databases/<database-name>

GCLOUD CMD FOR READER JOB:
gcloud dataflow flex-template run smt-reverse-replication-reader-2024-01-05t10-33-56z --project=<project> --region=<region> --template-file-gcs-location=<template location>  --parameters sessionFilePath=<session path>,windowDuration=10s,filtrationMode=forward_migration,skipDirectoryName=skip,instanceId=<spanner instance id>,spannerProjectId=<spanner-project-id>,metadataDatabase=rev_repl_metadata,gcsOutputDirectory=<gcs path>,metadataTableSuffix=,runMode=regular,metadataInstance=<spanner instance>,startTimestamp=,sourceShardsFilePath=<shard file path>,changeStreamName=reverseReplicationStream,databaseId=<spanner database name>,runIdentifier=2024-01-05t10-33-56z --num-workers=5 --worker-machine-type=n2-standard-4 --additional-experiments=use_runner_v2

Launched reader job:  id:"<>" project_id:"<>" name:"<>" current_state_time:{} create_time:{seconds:<> nanos:<>} location:"<region>" start_time:{seconds:<> nanos:<>}

GCLOUD CMD FOR WRITER JOB:
gcloud dataflow flex-template run smt-reverse-replication-writer-2024-01-05t10-33-56z --project=<project> --region=<region> --template-file-gcs-location=<template location> --parameters sourceShardsFilePath=<shard file path>,metadataTableSuffix=,GCSInputDirectoryPath=<gcs path>,metadataDatabase=rev_repl_metadata,sessionFilePath=<session file path>,sourceDbTimezoneOffset=+00:00,spannerProjectId=<spanner project id>,metadataInstance=<metadata instance>,runMode=regular,runIdentifier=2024-01-05t10-33-56z --num-workers=5 --worker-machine-type=n2-standard-4

Launched writer job:  id:"<>" project_id:"<>" name:"<>" current_state_time:{} create_time:{seconds:<> nanos:<>} location:"<region>" start_time:{seconds:<> nanos:<>}

```

### Custom Jar

In order to specify custom shard identification function, custom jar and class names need to give. The command to do that is below:

```
go run reverse-replication-runner.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<spanner-instance> -dbName=<spanner-database> -sourceShardsFilePath=gs://bucket-name/shards.json -sessionFilePath=gs://bucket-name/session.json -gcsPath=gs://bucket-name/<directory> -readerShardingCustomJarPath=gs://bucket-name/custom.jar -readerShardingCustomClassName=com.custom.classname
``` 

The sample reader job gcloud command for the same

```
gcloud dataflow flex-template run smt-reverse-replication-reader-2024-01-05t10-33-56z --project=<project> --region=<region> --template-file-gcs-location=<template location>  --parameters sessionFilePath=<session path>,windowDuration=10s,filtrationMode=forward_migration,skipDirectoryName=skip,instanceId=<spanner instance id>,spannerProjectId=<spanner-project-id>,metadataDatabase=rev_repl_metadata,gcsOutputDirectory=<gcs path>,metadataTableSuffix=,runMode=regular,metadataInstance=<spanner instance>,startTimestamp=,sourceShardsFilePath=<shard file path>,changeStreamName=reverseReplicationStream,databaseId=<spanner database name>,runIdentifier=2024-01-05t10-33-56z,shardingCustomJarPath=<jar path>,shardingCustomClassName=<custom class name> --num-workers=5 --worker-machine-type=n2-standard-4 --additional-experiments=use_runner_v2
```


### Network and Subnetwork specification

If the dataflow workers need to be run on a different network and subnetwork with custom network tags, sample command is below.
Note that specifying a network or subnetwork results in the Dataflow workers using the private IP addresses.

```
go run reverse-replication-runner.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<spanner-instance> -dbName=<spanner-database> -sourceShardsFilePath=gs://bucket-name/shards.json -sessionFilePath=gs://bucket-name/session.json -gcsPath=gs://bucket-name/<directory> -networkTags=test -vpcNetwork=<network> vpcSubnetwork=<subnetwork>

```

The sample reader job gcloud command for the same

```
gcloud dataflow flex-template run smt-reverse-replication-reader-2024-01-05t10-33-56z --project=<project> --region=<region> --template-file-gcs-location=<template location>  --disable-public-ips 
 --subnetwork=https://www.googleapis.com/compute/v1/projects/<project name>/regions/<region name>/subnetworks/<subnetwork name>
--parameters sessionFilePath=<session path>,windowDuration=10s,filtrationMode=forward_migration,skipDirectoryName=skip,instanceId=<spanner instance id>,spannerProjectId=<spanner-project-id>,metadataDatabase=rev_repl_metadata,gcsOutputDirectory=<gcs path>,metadataTableSuffix=,runMode=regular,metadataInstance=<spanner instance>,startTimestamp=,sourceShardsFilePath=<shard file path>,changeStreamName=reverseReplicationStream,databaseId=<spanner database name>,runIdentifier=2024-01-05t10-33-56z --num-workers=5 --worker-machine-type=n2-standard-4 --additional-experiments=use_runner_v2,use_network_tags=test,use_network_tags_for_flex_templates=test

```

The sample writer job gcloud command for the same

```
gcloud dataflow flex-template run smt-reverse-replication-writer-2024-01-05t10-33-56z --project=<project> --region=<region> --template-file-gcs-location=<template location> --disable-public-ips 
 --subnetwork=https://www.googleapis.com/compute/v1/projects/<project name>/regions/<region name>/subnetworks/<subnetwork name> --parameters sourceShardsFilePath=<shard file path>,metadataTableSuffix=,GCSInputDirectoryPath=<gcs path>,metadataDatabase=rev_repl_metadata,sessionFilePath=<session file path>,sourceDbTimezoneOffset=+00:00,spannerProjectId=<spanner project id>,metadataInstance=<metadata instance>,runMode=regular,runIdentifier=2024-01-05t10-33-56z --num-workers=5 --worker-machine-type=n2-standard-4 --additional-experiments=use_network_tags=test,use_network_tags_for_flex_templates=test

```

### Custom Names Prefix

If a separate prefix is needed for the dataflow job, sample command is below:

```
go run reverse-replication-runner.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<spanner-instance> -dbName=<spanner-database> -sourceShardsFilePath=gs://bucket-name/shards.json -sessionFilePath=gs://bucket-name/session.json -gcsPath=gs://bucket-name/<directory> -jobNamePrefix=rr
``` 

This does not change the gcloud commands, just that the Dataflowjob names have the supplied prefix.


### Tune Dataflow Configs

```
go run reverse-replication-runner.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<spanner-instance> -dbName=<spanner-database> -sourceShardsFilePath=gs://bucket-name/shards.json -sessionFilePath=gs://bucket-name/session.json  -gcsPath=gs://bucket-name/<directory> -machineType=e2-standard-2 -readerWorkers=10 -writerWorkers=8
``` 
These impact the below job parameters:
--num-workers
--worker-machine-type

### Resuming jobs

When the reverse replication Dataflow jobs are launched, they are assigned a run identifier. If for some reason, the jobs are stopped, then they can be resumed to process from the point where they left off - provided that the run identifier is the same.

Sample command for the same:

```
go run reverse-replication-runner.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<spanner-instance> -dbName=<spanner-database> -sourceShardsFilePath=gs://bucket-name/shards.json -sessionFilePath=gs://bucket-name/session.json -gcsPath=gs://bucket-name/<directory> -runIdentifier=<original run identifier> -skipChangeStreamCreation=true -skipMetadataDatabaseCreation=true -readerRunMode=resume  -writerRunMode=resumeAll
```
These impact the below job parameters:
runIdentifier
runMode

### Reprocessing error shards

While the pipeline progresses, if there are errors writing to specific shards, the writer job halts processing those shards. The user should fix the errors and then another Dataflow job can be launched to start processing only the erred shards. The run identifer in this case should be same as the original one.

Sample command for the same:

```
go run reverse-replication-runner.go -projectId=<project-id> -dataflowRegion=<region> -instanceId=<spanner-instance> -dbName=<spanner-database> -sourceShardsFilePath=gs://bucket-name/shards.json -sessionFilePath=gs://bucket-name/session.json -gcsPath=gs://bucket-name/<directory> -runIdentifier=<original run identifier> -skipChangeStreamCreation=true -skipMetadataDatabaseCreation=true -jobsToLaunch=writer  -writerRunMode=resumeFailed
```

