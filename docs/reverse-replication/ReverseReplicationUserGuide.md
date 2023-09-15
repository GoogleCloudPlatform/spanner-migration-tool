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
2. Removing forward migrated changes
3. Cloud Spanner being distributed database, the changes captured must be temporally ordered before writing to a single source database
4. Transforming Cloud Spanner data to source database schema
5. Writing to source database

These steps are achieved by two Dataflow jobs, along with an interim buffer which holds the ordered changes.

![Architecture](https://services.google.com/fh/files/misc/reversereploverview.png)


*Note that the buffer used is the [Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/overview). Kafka is experimentally supported and requires manual setup, which is not discussed in this guide. [Contact us](#contact-us) for using Kafka.*

## Before you begin

A few prerequisites must be considered before starting with reverse replication.

1. Make sure that there is network connectivity between source database and your GCP project on which the Dataflow jobs will run.Ensure the Dataflow worker IPs can access the MySQL IPs.
2. Ensure that Dataflow permissions are present.[Basic permissions](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#before_you_begin:~:text=Grant%20roles%20to%20your%20Compute%20Engine%20default%20service%20account.%20Run%20the%20following%20command%20once%20for%20each%20of%20the%20following%20IAM%20roles%3A%20roles/dataflow.admin%2C%20roles/dataflow.worker%2C%20roles/bigquery.dataEditor%2C%20roles/pubsub.editor%2C%20roles/storage.objectAdmin%2C%20and%20roles/artifactregistry.reader) and [Flex template permissions](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#permissions).
3. Ensure the compute engine service account has the following permissions:
    - roles/pubsub.subscriber
    - roles/pubsub.publisher
    - roles/spanner.databaseUser
4. Ensure the authenticated user launching reverse replication has the following permissions: (this is the user account authenticated for the Spanner Migration Tool and not the service account)
    - roles/spanner.databaseUser
    - roles/pubsub.editor
    - roles/dataflow.developer
5. Ensure that [golang](https://go.dev/dl/) (version 1.18 and above) is setup on the machine from which reverse replication flow will be launched.
6. Ensure that gcloud authentication is done,refer [here](./RunnigReverseReplication.md#before-you-begin).
7. Ensure that the target Spanner instance ready.
8. Ensure that that [session file](./RunnigReverseReplication.md#files-generated-by-spanner-migration-tool) is uploaded to GCS (this requires a schema conversion to be done).
9. [Source shards file](./RunnigReverseReplication.md#sample-sourceshards-file) already uploaded to GCS.
10. Resources needed for reverse replication incur cost. Make sure to read [cost](#cost).

## Launching reverse replication

Currently, the reverse replication flow is launched manually via a script. The details for the same are documented [here](./RunnigReverseReplication.md#reverse-replication-setup).

## Observe, tune and troubleshoot

### Tracking progress

There are various progress points in the pipeline. Below sections detail how to track progress at each of them.

#### Verify that change stream has data

Unless there is change stream data to stream from Spanner, nothing will be reverse replicated. The first step is to verify that change stream has data. Refer [here](https://cloud.google.com/spanner/docs/change-streams/details#query) on how to check this.


#### Metrics for Dataflow job that writes from Spanner to Sink

The progress of the Dataflow jobs can be tracked via the Dataflow UI.

The last step gives an approximation of where the step is currently - the Data Watermark would give indication of Spanner commit timestamp that is guaranteed to be processed. On the Dataflow UI, click on JobGraph and scroll to the last step, as shown below. Click on the last step and the metrics should be visible on the right pane.

![Metrics](https://services.google.com/fh/files/misc/sourcetosinkmetrics.png)


#### Pub/Sub metrics

Track the Pub/Sub topic [metrics](https://cloud.google.com/pubsub/docs/monitor-topic) to verify that there is inflow of messages by checking the 'Published Messages' metric.

Note that subscription [metrics](https://cloud.google.com/pubsub/docs/monitor-subscription) can be verified to check if the dataflow job that writes to source database is reading from Pub/Sub as expected.

#### Metrics for Dataflow job that writes to source database

The Dataflow job that writes to source database exposes per shard metric like so, which should be visible on the right pane titled 'Job Info'.

![Metrics](https://services.google.com/fh/files/misc/orderedbuffertosourcemetrics.png)

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

#### Verifying the data in the source database

To confirm that the records have indeed been written to the source database, best approach is to check the record count on the source database, if that matches the expected value. Note that verifying data takes more than just record count matching. The suggested tool for the same is [here](https://github.com/GoogleCloudPlatform/professional-services-data-validator).

### Troubleshooting

Following are some scenarios and how to handle them.

#### Dataflow job does not start

1. Check that permission as listed in [prerequisites](#before-you-begin) section are present.
2. Check the DataFlow logs since they are an excellent way to understand if something is not working as expected.
If you observe that the pipeline is not making expected progress, check the Dataflow logs for any errors.For Dataflow related errors, please refer [here](https://cloud.google.com/dataflow/docs/guides/troubleshooting-your-pipeline) for troubleshooting. Note that sometimes logs are not visible in Dataflow, in such cases, follow these suggestions.

![DataflowLog](https://services.google.com/fh/files/misc/dataflowlog.png)



#### Records are not getting reverse replicated

In this case, check if you observe the following:

- ***The watermark of the Spanner to Sink pipeline does not advance***

    This happens when the job is hit with a huge backlog, that leads to infinite loop. The recovery steps are covered [here](#recovery-steps-for-the-infinte-loop).

- ***PubSub message count is not decreasing and the same data is being written back to source repeatedly***

    This happens when the time to write all messages to source database and send an ACK to PubSub exceeds the deadline. Ensure the ACK deadline for the subscriptions are high enough (10 minutes is the highest value, consider bumping it to that). If still facing this issue, consider moving the Dataflow job writing to source database geographically closer to the source database.

- ***There is data in change stream yet not present in Pub/Sub***

    Records of below nature are dropped from reverse replication. Check the Dataflow logs to see if they are dropped.
    1. Records which are forward migrated. 
    2. Shard Id based routing could not be performed since the shard id value could not be determined.
    3. The record was deleted on Cloud Spanner and the deleted record was removed from Cloud Spanner due to lapse of retention period by the time the record was to be reverse replicated.

- ***There is data in Pub/Sub yet not present in source database***

  Records of below nature are dropped from reverse replication. Check the Dataflow logs to see if they are dropped.

     1.Records for which primary key cannot be determined on the source database.This can happen when the source database table does not have a primary key, or the primary key value was not present in the change stream data, or the record was deleted on Cloud Spanner and the deleted record was removed from Cloud Spanner due to lapse of retention period by the time the record was to be reverse replicated.

#### There is higher load than the expected QPS on  spanner instance post cutover

1. Change steams query incurs load on spanner instance, consider scaling up if it becomes a bottleneck.


2. If the forward migration is still running post cutover, the incoming writes on Spanner that are reverse-replicated to the source get forward migrated again. This can cause the load on Spanner to be almost double the expected QPS, as each write will get reflected twice. Also, it could lead to transient inconsistencies in data under certain cases. To avoid this, stop/delete the forward migration pipeline post cutover. If the forward pipeline is required, add custom filtration rules and build a custom forward migration dataflow template.

### Retry

For both the Dataflow jobs, once an error is encountered for a given shard, then procesing is stopped for that shard to preserve ordering.To recover,rerun the job.The jobs are idempotent and it's safe to rerun them.

The command to run the Dataflow jobs should be available when launching the Dataflow jobs via launcher script.

Example command for the Spanner to Sink job

```code
gcloud dataflow flex-template run ordering-fromspanner \
  --project <project name> \
  --region <region name> \
  --template-file-gcs-location gs://dataflow/templates/flex/Spanner_Change_Streams_to_Sink \
--additional-experiments=use_runner_v2 \
  --parameters "changeStreamName=<stream name>" \
  --parameters "instanceId=<instance name>" \
  --parameters "databaseId=<database name>" \
  --parameters "spannerProjectId=<project id>" \
  --parameters "metadataInstance=<metadata instance>" \
  --parameters "metadataDatabase=<metadata database>" \
  --parameters "sinkType=pubsub" \
  --parameters "pubSubDataTopicId=projects/<project name>/topics/<topic name>" \
  --parameters "pubSubErrorTopicId=projects/<project name>/topics/<topic name>" \
  --parameters "pubSubEndpoint=<end point name>:443" \
--parameters "sessionFilePath=<gcs path to session json file>"

```

Example command for the writing to source database job

```code
gcloud beta dataflow flex-template run writes-tosql  --project=<project name>    --region=<region name>     --template-file-gcs-location=gs://dataflow/templates/flex/Ordered_Changestream_Buffer_to_Sourcedb --num-workers=1  --worker-machine-type=n2-standard-64 --additional-experiments=use_runner_v2 --parameters sourceShardsFilePath=<path to source shards file>,sessionFilePath=<gcs path to session json file>,bufferType=pubsub,pubSubProjectId=<project name>

```

## Reverse Replication Limitations

The following sections list the known limitations that exist currently with the Reverse Replication flows:

### Dataflow job of Spanner to Sink getting stuck in infinte loop
The Dataflow job that reads the change streams and writes to PubSub gets stuck in infinite loop retrying the same set of records during certain scenarios.These scenario can arise when there are a lot of changestream records to be read in a short interval of time, which occurs in  the following situations:
1. There is an unexpected spike on Spanner
2. The pipeline is started with a date in past ( due to issues that required downtime such as bug fix )
Currently, there is no way to revert this within the same job. More details and recovery steps below.

#### Recovery steps for the infinte loop
1. The user must track the last handled window that was successfully processed - this can be obtained from the Dataflow logs or by checking the watermark of the job stage.
2. Specify a different partition metadata database or drop the previous metadata tables  - since watermarks for a given partition are persisted and hence will be read from that point onwards. So the pipeline must begin with a clean partition metadata database.Note that the metadata tables can be dropped by giving DROP TABLE\<table name\> statements in the Cloud UI.
3. The current pipeline must be updated - the user must specify the start time of change stream query as the last successfully processed timestamp and the end time of the change stream as the time that would result in ~1GB writes. Example, if average record size is 1KB, then 10,00,000 records should be processed in a window and if the TPS during that window was 20K, then the window must end at 50 second. Detailed steps to update the Dataflow job are given here: https://cloud.google.com/dataflow/docs/guides/updating-a-pipeline#gcloud-cli
4. Once this window is processed, the pipeline must be updated with next set of start and end times, until the pipeline catches up and the finally the end timestamp need not be passed.


### Reverse transformations
Reverse transformation can not be supported for following scenarios out of the box:
1. The table in original database has single column while in Spanner it’s split into multiple - example POINT to X,Y coordinates
2. Adding column in Spanner that does not exist in source - in this case the column cannot be replicated
3. Deleting column in Spanner that is mandatory in source
4. Spanner PK is UUID while source PK is auto-increment key 
5. Spanner table has more columns as part of PK than the source 
6. Spanner columns have greater length of string columns than source 
7. Spanner columns have different data type than source 
8. CLOB will not be read from GCS and put in source 
9. DELETES on Spanner that have Primary key columns different from the Source database column - such records will be dropped
10. Primary key of the source table cannot be determined - such records will be dropped
In the above cases, custom code will need to be written to perform reverse transformation. The source code can be taken from https://github.com/aksharauke/DataflowTemplates/tree/main/v2/ordered-changestream-buffer-to-sourcedb and extended to write these custom transforms.

## Best practices

1. Avoid backlog build up of Spanner writes before starting the reverse replication. Start the reverse replication pipeline just before cutover of the first shard.

2. Set the chagne stream retention period to maximum value of 7 days to avoid any data loss.

3. Use the launcher script to create the necessary GCP resources and avoid creating them manually.


## Customize

The Dataflow jobs can be customized. 
Some use cases could be:
1. To customize the logic to filter records from reverse replication.
2. To handle some custom reverse transformation scenarios.
3. To customize shard level routing.

To customize, checkout the open source template, add the custom logic, build and launch the open source template.

Refer to [Spanner Change Streams to Sink template](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/spanner-change-streams-to-sink#readme) on how to build and customize this. 

Refer to [Ordered Changestream Buffer to Sourcedb](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/ordered-changestream-buffer-to-sourcedb#readme) on how to build and customize this.


## Cost

1. Cloud Spanner change stream incur additional storage requirement, refer [here](https://cloud.google.com/spanner/docs/change-streams#data-retention).
2. For Dataflow pricing, refer [here](https://cloud.google.com/dataflow/pricing)
3. For Pub/Sub pricing, refer [here](https://cloud.google.com/pubsub/pricing).

## Contact us

Have a question? We are [here](https://cloud.google.com/support).
