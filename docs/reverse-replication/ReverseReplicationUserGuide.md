---
layout: default
title: User guide
parent: Reverse Replication
nav_order: 1
---

# Cloud Spanner Reverse Replication User Guide

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Overview

### Background

Migrating a database is a complex affair, involving changes to the schema, converting the application, tuning for performance, ensuring minimal downtime and completeness during data migration. It is possible that after migration and cutover, issues/inconsistent performance are encountered on the target (Cloud Spanner) requiring a fallback to the original source database with minimal disruption to the service. Reverse replication enables this fallback by replicating data written on Cloud Spanner back to the source database.This allows the application to point to the source and continue serving requests correctly.

Reverse replication could also be used to replicate the Cloud Spanner writes to a different database, other than the source database, for performing reconciliation, validations and reporting.

### How it works

Reverse replication flow involves below steps:

1. Reading the changes that happened on Cloud Spanner using [Cloud Spanner change streams](https://cloud.google.com/spanner/docs/change-streams)
2. Removing forward migrated changes ( if configured to filter )
3. Cloud Spanner being distributed database, the changes captured must be temporally ordered before writing to a single source database
4. Transforming Cloud Spanner data to source database schema
5. Writing to source database

These steps are achieved by two Dataflow jobs, along with an interim buffer which holds the change stream records.

![Architecture](https://services.google.com/fh/files/misc/reversereplicationgcsgeneric.png)


## Before you begin

A few prerequisites must be considered before starting with reverse replication.

1. Ensure network connectivity between the source database and your GCP project, where your Dataflow jobs will run.
  - Allowlist Dataflow worker IPs on the MySQL instance so that they can access the MySQL IPs.
  - Check that the MySQL credentials are correctly specified in the [source shards file](./RunnigReverseReplication.md#sample-sourceshards-file).
  - Check that the MySQL server is up.
  - The MySQL user configured in the [source shards file](./RunnigReverseReplication.md#sample-sourceshards-file) should have [INSERT](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_insert), [UPDATE](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_update) and [DELETE](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_delete) privileges on the database.
2. Ensure that Dataflow permissions are present.[Basic permissions](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#before_you_begin:~:text=Grant%20roles%20to%20your%20Compute%20Engine%20default%20service%20account.%20Run%20the%20following%20command%20once%20for%20each%20of%20the%20following%20IAM%20roles%3A%20roles/dataflow.admin%2C%20roles/dataflow.worker%2C%20roles/bigquery.dataEditor%2C%20roles/pubsub.editor%2C%20roles/storage.objectAdmin%2C%20and%20roles/artifactregistry.reader) and [Flex template permissions](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#permissions).
3. Ensure the compute engine service account has the following permission:
    - roles/spanner.databaseUser
    - roles/secretManager.secretAccessor
    - roles/secretmanager.viewer
4. Ensure the authenticated user launching reverse replication has the following permissions: (this is the user account authenticated for the Spanner Migration Tool and not the service account)
    - roles/spanner.databaseUser
    - roles/dataflow.developer
5. Ensure that [golang](https://go.dev/dl/) (version 1.18 and above) is setup on the machine from which reverse replication flow will be launched.
6. Ensure that gcloud authentication is done,refer [here](./RunnigReverseReplication.md#before-you-begin).
7. Ensure that the target Spanner instance ready.
8. Ensure that that [session file](./RunnigReverseReplication.md#files-generated-by-spanner-migration-tool) is uploaded to GCS (this requires a schema conversion to be done).
9. [Source shards file](./RunnigReverseReplication.md#sample-sourceshards-file) already uploaded to GCS.
10. Resources needed for reverse replication incur cost. Make sure to read [cost](#cost).
11. Reverse replication uses shard identifier column per table to route the Spanner records to a given source shard.The column identified as the sharding column needs to be selected via Spanner Migration Tool when performing migration.The value of this column should be the logicalShardId value specified in the [source shard file](./RunnigReverseReplication.md#sample-sourceshards-file).In the event that the shard identifier column is not an existing column,the application code needs to be changed to populate this shard identifier column when writing to Spanner.
12. The reverse replication pipelines use GCS as data buffer, this GCS bucket needs to be created before starting the reverse replication flows.

## Launching reverse replication

Currently, the reverse replication flow is launched manually via a script. The details for the same are documented [here](./RunnigReverseReplication.md#reverse-replication-setup).

## Observe, tune and troubleshoot

### Tracking progress

There are various progress points in the pipeline. Below sections detail how to track progress at each of them.

#### Verify that change stream has data

Unless there is change stream data to stream from Spanner, nothing will be reverse replicated. The first step is to verify that change stream has data. Refer [here](https://cloud.google.com/spanner/docs/change-streams/details#query) on how to check this.


#### Metrics for Dataflow job that writes from Spanner to GCS

The progress of the Dataflow jobs can be tracked via the Dataflow UI.

The last step gives an approximation of where the step is currently - the Data Watermark would give indication of Spanner commit timestamp that is guaranteed to be processed. On the Dataflow UI, click on JobGraph and scroll to the last step, as shown below. Click on the last step and the metrics should be visible on the right pane.

![Metrics](https://services.google.com/fh/files/misc/readermetrics.png)

In addition, there are following application metrics exposed by the job:

| Metric Name                           | Description                                                                                                                      |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| data_record_count | The number of change stream records read |
| num_files_written_\<logical shard name\>| Number of files successfully written for the shard |

The progress of files created per shard is also captured in the shard_file_create_progress table, which gets created in the metadata database specified when starting the job.


#### Metrics for Dataflow job that writes to source database

The Dataflow job that writes to source database exposes the following per shard metrics:

| Metric Name                           | Description                                                                                                                      |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| file_read_\<logical shard name\>| Number of files successfully read for the shard |
| records_read_from_gcs_\<logical shard name\>| Number of records read from GCS for the shard |
| records_processed_\<logical shard name\> | Number of records successfully written for the shard
|
|replication_lag_in_seconds_\<logical shard name\>| Replication lag min,max and count value for the shard|
| metadata_file_create_lag_retry_\<logical shard name\> | Count of file lookup retries done when the job that writes to GCS is lagging |
| mySQL_retry_\<logical shard name\> | Number of retries done when MySQL is not reachable|

These can be used to track the pipeline progress.
However, there is a limit of 100 on the total number of metrics per project. So if this limit is exhausted, the Dataflow job will give a message like so:

![ExhaustedMetrics](https://services.google.com/fh/files/misc/metricexhausted.png)

In such cases, the metrics can be viewed on the [Cloud Monitoring](https://cloud.google.com/monitoring/docs/monitoring-overview) console by writing a query:

![MQL](https://services.google.com/fh/files/misc/monitoringql.png)

Sample query

```code
fetch dataflow_job
| metric
'dataflow.googleapis.com/job/user_counter'
| filter
(resource.job_name == 'rr-demo-tosql')
| group_by 1m , [ value_user_counter_mean:
mean(value.user_counter)]
| every 1m
```

Metrics visible on Dataflow UI  can also be queried via REST,official document [here](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/getMetrics?apix_params=%7B%22projectId%22%3A%22span-cloud-testing%22%2C%22location%22%3A%22us-east1%22%2C%22jobId%22%3A%222023-06-06_05_20_27-10999367971891038895%22%7D).

The progress of files created per shard is also captured in the shard_file_process_progress table, which gets created in the metadata database specified when starting the job.

#### Verifying the data in the source database

To confirm that the records have indeed been written to the source database, best approach is to check the record count on the source database, if that matches the expected value. Note that verifying data takes more than just record count matching. The suggested tool for the same is [here](https://github.com/GoogleCloudPlatform/professional-services-data-validator).

#### Tracking which shards are lagging

The following sample SQL gives the shards which are yet to catchup in the writer job. The SQL needs to be fired on the metatdata database. Replace the run_id with the relevant run identifier and the window interval with appropriate value.

```
select w.shard,r.shard,r.created_upto , w.file_start_interval from shard_file_create_progress r , shard_file_process_progress w where  r.run_id = w.run_id and r.shard = w.shard and
TIMESTAMP_DIFF(r.created_upto , w.file_start_interval, SECOND)  > 10
and r.run_id="2024-03-02t05-26-43z"
```
### Troubleshooting

Following are some scenarios and how to handle them.

#### Dataflow job does not start

1. Check that permission as listed in [prerequisites](#before-you-begin) section are present.
2. Check the DataFlow logs, since they are an excellent way to understand if something is not working as expected.
If you observe that the pipeline is not making expected progress, check the Dataflow logs for any errors.For Dataflow related errors, please refer [here](https://cloud.google.com/dataflow/docs/guides/troubleshooting-your-pipeline) for troubleshooting. Note that sometimes logs are not visible in Dataflow, in such cases, follow these suggestions.

![DataflowLog](https://services.google.com/fh/files/misc/dataflowlog.png)



#### Records are not getting reverse replicated

In this case, check if you observe the following:


- ***There is data in change stream yet not present in GCS***

    Records of below nature are dropped from reverse replication. Check the Dataflow logs to see if they are dropped.
    1. Records which are forward migrated. 
    2. Shard Id based routing could not be performed since the shard id value could not be determined.
    3. The record was deleted on Cloud Spanner and the deleted record was removed from Cloud Spanner due to lapse of retention period by the time the record was to be reverse replicated.
    4. Check the data_seen and shard_file_create_progress tables created in the metadata database. An entry in the data_seen table means that change record for read for the given interval for the given shard. If no change record was generated for the interval, then no file is generated. The shard_file_create_progress table indicates the maximum interval until which the files have been generated for the shard at that point. If the file creation interval is lesser than the expected interval, then wait for the pipeline to process the change records.
    5. Check for issues in the dataflow job. This can include scaling issues, CPU utilization being more than 70% consistently. This can be checked via [CPU utilization](https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf#cpu-use) section on the Dataflow job UI.Check for any errors in the jobor worker logs which could indicate restarts. Sometimes a worker might restart causing a delay in record processing. The CPU utlization would show multiple workers during the restart period. The number of workers could also be viewed via [here](https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf#autoscaling).
    6. When working with session file based shard identification logic, if the table of the change record does not exist in the session file, such records are written to skip directory and not reverse replicated.

- ***There is data in GCS yet not present in source database***

   Check worker logs to ensure that records are being read from GCS. Filter the logs based on logical shard id of the shard you want to check. It should have messages like below, which indicate records are being read from GCS.


    ![DataflowLog](https://services.google.com/fh/files/misc/recordsreadfrompubsub.png)

    Check for logs to see if there are any Connection exception warnings like below. This means that the source database is not reachable and the job keeps retrying to connect, hence nothing gets written to the source database. In such case, please ensure that the [prerequisite](#before-you-begin) of connectivity between source database and Dataflow workers is met.
    ![DataflowLog](https://services.google.com/fh/files/misc/connectionretry.png)

   Note that in case of connection exceptions, the mySQL_retry_\<logical shard name\> metric would keep incrementing to indicate that connection is being retired.

  Check the Dataflow logs to see if records are being dropped. This can happen for records for which primary key cannot be determined on the source database. This can happen when:

  1. The source database table does not have a primary key
  2. The primary key value was not present in the change stream data
  3. When there is no data written to Spanner for a given interval for a given shard, no file is created in GCS. In such a case, the interval is skipped by the writer Dataflow job. This can be verified in the logs by searching for the text *skipping the file*. If a file is marked as skipped in the logs but it exists in GCS - this indicates a data loss scenario - please raise a bug.
  4. Check the shard_file_process_progress table in the metadata database. If it is lagging, then wait for the pipeline to catch up so such that data gets reverse replicated.

    
#### There is higher load than the expected QPS on  spanner instance post cutover

1. Change steams query incurs load on spanner instance, consider scaling up if it becomes a bottleneck.


2. If the forward migration is still running post cutover, the incoming writes on Spanner that are reverse-replicated to the source get forward migrated again. This can cause the load on Spanner to be almost double the expected QPS, as each write will get reflected twice. Also, it could lead to transient inconsistencies in data under certain cases. To avoid this, stop/delete the forward migration pipeline post cutover. If the forward pipeline is required, add custom filtration rules and build a custom forward migration dataflow template.

### Resuming from failures

The reader Dataflow job stops when any error is encountered. The writer dataflow job halts processing a shard if there is error encountered for the shard, this is to ensure ordering of writes does not break.

The metadata tables keep track of progress made by the Dataflow templates. This helps to start the Dataflow jobs from where they left off. 

The command to run the Dataflow jobs should be available when launching the Dataflow jobs via launcher script.The arguments are similar to what was passed in the launcher [script](./RunnigReverseReplication.md#arguments).

Please refer dataflow [documentation](https://cloud.google.com/dataflow/docs/guides/routes-firewall#internet_access_for) on network options.

When disabling the public IP for Dataflow, the option below should be added to the command line:

```
--disable-public-ips 
```

When providing subnetwork, give the option like so:

```
--subnetwork=https://www.googleapis.com/compute/v1/projects/<project name>/regions/<region name>/subnetworks/<subnetwork name>
```

#### Retry of Reverse Replication jobs

In order to resume the reader job, it should be started with run mode as **resume** and the runIdentifier should be same as that of the original job.

In order to resume the writer job for all shards the run mode should be **resumeAll** and the runIdentifier should be same as that of the original job.

Example command for resume is [here](RunnigReverseReplication.md#resuming-jobs).

In order to process only the failed shards in the writer job, the run mode should be **resumeFailed** and the runIdentifier should be same as that of the original job.

Example command for the same is [here](RunnigReverseReplication.md#reprocessing-error-shards)


In order to process only certain failed shards, update the status as REPROCESS in the shard_file_process_progress table for those shards and launch writer job, the run mode should be **reprocess** and the runIdentifier should be same as that of the original job.

In order to resume processing of only the successful shards in the writer job, the run mode should be **resumeSuccess** and the runIdentifier should be same as that of the original job.


Note: Additional optional parameters for the reader job are [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-change-streams-to-sharded-file-sink/README_Spanner_Change_Streams_to_Sharded_File_Sink.md#optional-parameters).


Note: Additional optional parameters for the writer job are [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/gcs-to-sourcedb/README_GCS_to_Sourcedb.md#optional-parameters).

### Ignorable errors

Dataflow retries most of the errors. Following errors if shown up in the Dataflow UI can be ignored.

#### Reader job

1. File not found exception like below. These are thrown at the time Dataflow workers auto-scale and the work gets reassigned among the workers.

```
java.io.FileNotFoundException: Rewrite from <GCS bucket name>/.temp-beam/<file name> to <GCS file path> has failed
```

2. Spanner DEADLINE_EXCEEDED exception.

3. GC thrashing exception like below

```
Shutting down JVM after 8 consecutive periods of measured GC thrashing.
```

4. GCS throttling can slow down writes for a while. Below exception results during that period and can be ignored.

```
Operation ongoing in bundle process_bundle-<bundle number> for PTransform{id=Write To GCS/Write rows to output writeDynamic/WriteFiles/WriteShardedBundlesToTempFiles/WriteShardsIntoTempFiles-ptransform-46, name=Write To GCS/Write rows to output writeDynamic/WriteFiles/WriteShardedBundlesToTempFiles/WriteShardsIntoTempFiles-ptransform-46, state=process} for at least 05m20s without outputting or completing
```

```
java.io.IOException: Error executing batch GCS request
```

#### Writer job

1. To preserve ordering, the writer job processes files in incrementing window intervals. If the reader job is lagging in creating files, the writer job waits for the expected file for a given window to be written to GCS. In such cases, below messages and logged and can be ignored.

```
Operation ongoing in step Write to source for at least 05m00s without outputting or completing in state process-timers in thread DataflowWorkUnits-11418 with id 1891593
  at java.base@11.0.20/java.lang.Thread.sleep(Native Method)
  at app//com.google.cloud.teleport.v2.templates.utils.GCSReader.checkAndReturnIfFileExists
```

2. Large amount of logging can result in below. This does not halt the Dataflow job from processing.

```
Throttling logger worker. It used up its 30s quota for logs in only 17.465s
```

#### Common to both jobs

1. Ephemeral network glitches results in below ignorable error.

```
StatusRuntimeException: UNAVAILABLE: ping timeout
```

## Reverse Replication Limitations

  The following sections list the known limitations that exist currently with the Reverse Replication flows:

  1. Currently only MySQL source database is supported.
  2. If forward migration and reverse replication are running in parallel, there is no mechanism to prevent the forward migration of data that was written to source via the reverse replicaiton flow. The impact of this is unnecessary processing of redundant data.
  3. Certain transformations are not supported, below section lists those:

### Reverse transformations
Reverse transformation can not be supported for following scenarios out of the box:
1. The table in original database has single column while in Spanner it’s split into multiple - example POINT to X,Y coordinates
2. Adding column in Spanner that does not exist in source - in this case the column cannot be replicated
3. Deleting column in Spanner that is mandatory in source
4. Spanner PK is UUID while source PK is auto-increment key 
5. Spanner table has more columns as part of PK than the source - in this case the source records having the same values as the partial primary keys are updated 
6. Spanner columns have greater length of string columns than source 
7. Spanner columns have different data type than source 
8. CLOB will not be read from GCS and put in source 
9. DELETES on Spanner that have Primary key columns different from the Source database column - such records will be dropped
10. Primary key of the source table cannot be determined - such records will be dropped

In the above cases, custom code will need to be written to perform reverse transformation.Refer the [customization](#customize) section for the source code to extended and write these custom transforms.

## Best practices

1. Set the change stream retention period to maximum value of 7 days to avoid any data loss.

2. The change records get written to GCS in plain text, ensure that appropriate [access control](https://cloud.google.com/storage/docs/access-control) exist on GCS to avoid inadvertant data access.

3. The Spanner TPS and [windowDuration](RunnigReverseReplication.md#arguments) decides how large a batch will be when writing to source. Perfrom benchmarks on expected production workloads and acceptable replication lag to fine tune the windowDuration.

4. The metrics give good indication of the progress of the pipeline, it is good to setup [dashboards](https://cloud.google.com/monitoring/charts/dashboards) to monitor the progress.

5. Create GCP bucket with [lifecycle](https://cloud.google.com/storage/docs/lifecycle) to handle auto deletion of the objects.

6. Use a different database for the metadata tables than the Spanner database to avoid load.

7. The default change stream monitors all the tables, if only a subset of tables needs reverse replication, create change stream manually before launching the script. When creating a change stream manually, use the NEW_ROW option, sample command below :
```
CREATE CHANGE STREAM allstream
FOR ALL OPTIONS (
retention_period = '7d',
value_capture_type = 'NEW_ROW'
);
```


## Customize

The Dataflow jobs can be customized. 
Some use cases could be:
1. To customize the logic to filter records from reverse replication.
2. To handle some custom reverse transformation scenarios.
3. To customize shard level routing.

To customize, checkout the open source template, add the custom logic, build and launch the open source template.

Refer to [Spanner Change Streams to Sharded File Sink template](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/spanner-change-streams-to-sharded-file-sink) on how to build and customize this. 

Refer to [GCS to Sourcedb](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/gcs-to-sourcedb) on how to build and customize this.

### Shard routing customization

In order to make it easier for users to customize the shard routing logic, the [Spanner Change Streams to Sharded File Sink template](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/spanner-change-streams-to-sharded-file-sink) accepts a GCS path that points to a custom jar and another input parameter that accepts the custom class name, which are used to invoke custom logic to perform shard identification.

Steps to perfrom customization:
1. Write custom shard id fetcher logic [CustomShardIdFetcher.java](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-custom-shard/src/main/java/com/custom/CustomShardIdFetcher.java). Details of the ShardIdRequest class can be found [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-migrations-sdk/src/main/java/com/google/cloud/teleport/v2/spanner/utils/ShardIdRequest.java).
2. Build the [JAR](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/spanner-custom-shard) and upload the jar to GCS
3. Invoke the reverse replication flow by passing the [custom jar path and custom class path](RunnigReverseReplication.md#custom-jar).
4. If any custom parameters are needed in the custom shard identification logic, they can be passed via the *readerShardingCustomParameters* input to the runner. These parameters will be passed to the *init* method of the custom class. The *init* method is invoked once per worker setup.



## Cost

1. Cloud Spanner change stream incur additional storage requirement, refer [here](https://cloud.google.com/spanner/docs/change-streams#data-retention).
2. For Dataflow pricing, refer [here](https://cloud.google.com/dataflow/pricing)
3. For GCS pricing, refer [here](https://cloud.google.com/storage/pricing).

## Contact us

Have a question? We are [here](https://cloud.google.com/support).
