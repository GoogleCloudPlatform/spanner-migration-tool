---
layout: default
title: Minimal downtime migration
parent: Troubleshooting
nav_order: 1
---

# Error handling
{: .no_toc }

This section gives information about how to track errors that have occurred during minimal downtime migration via metrics and how to retry the errored records.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Minimal Downtime Migration

The Dataflow job that handles minimal downtime migration runs in two mode:

- **regular**: This is the default mode, where the events streamed by Datastream are picked up and converted to Spanner compatible data types and applied to Spanner. It also does automatic retry of retryable errors and once the retry is exhausted, moves them to a dead letter queue (DLQ) directory in GCS. Permanent errors are also moved to the dead letter queue.
- **retryDLQ**: This mode looks at the DLQ and retries the events. This mode is ideal to run when all the permanent and/or retryable errors are fixed - for example any bug fix/ dependent data migration is complete.This mode only reads from DLQ and not from Datastream output.

## Current error scenarios

The following error scenarios are possible currently when doing low downtime migration:

1. If there is a foreign key constraint on a table - and that constraint got applied successfully on Spanner - then due to unordered processing by Datastream - the child table record comes before the parent table record and it fails with foreign key constraint violation.
1. Due to unordered processing of Datastream - for interleaved tables - it can happen that child table records arrive before the parent table record and it fails with the parent record not found error.
1. There could also be some intermittent errors from Spanner like deadline exceeded due a temporary resource impact.
1. Other SpannerExceptions - which are marked for retry
1. In addition, there is a possibility of severe errors that would require manual intervention. Examples of severe error could be error during transformation.

Points 1 to 4 above are retryable errors - the Dataflow job automatically retries them at intervals of 10 minutes for 500 times. In most cases, this should be good enough for the retryable records to succeed, however, even if after exhausting all the retries, these are not successful - then these records are marked as ‘severe' error category. Such ‘severe' errors can be retried later with a ‘retryDLQ' mode of the Dataflow job (discussed [below](#to-re-run-for-reprocessing-dlq-directory)).  
The following scenarios results in skipping of records, they are not really errors:

1. Invalid structure of records read from Datastream output
1. Table that existed in source but was dropped during schema conversion

Note there can be exceptions like invalid arguments to the Dataflow pipeline - these cause the pipeline to halt.

## Metrics

Migration progress can be tracked by monitoring the Dataflow job and following custom metrics are exposed:

### Metrics for Regular Run

| Metric Name                                   | Description                                                                                                                            |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Successful events                             | Total number of events successfully processed and applied to Spanner database                                                          |
| Retryable errors                              | The count of events that were errored out but will be retried                                                                          |
| Total permanent errors                        | The number of events that are errored out with non-retriable errors in addition to the number of errors after exhausting retries       |
| Conversion errors                             | Number of events that could not be converted to Spanner. This is a permanent error category.                                            |
| Skipped events                                | Number of events skipped from being written to spanner because the events were stale.                                                  |
| Other permanent errors                        | The remaining permanent errors.                                                                                                        |
| Transformed events                            | The number of events that were successfully transformed, including retries and permanent errors.                                       |
| Filtered events                               | The number of events that were filtered as a part of custom transformation.                                                          |
| Custom Transformation Exceptions              | The number of events that were errored out due to some exception in custom transformation jar.                                         |
| Total events processed                        | The number of events that were tried for forward migration, including retries and permanent errors.                                    |
| `apply_custom_transformation_impl_latency_ms` | Latency of applying custom transformation to the event.                                                                              |
| `elementsReconsumedFromDeadLetterQueue`       | The total number of events consumed from DLQ for retry.                                                                                |
| Replication lag system latency                | Time taken from events being read by Datastream to being written to Cloud Spanner. Time duration between `datastream_read_timestamp` and `write_timestamp` |
| Replication lag dataflow latency              | Time taken for events to get processed by the Dataflow pipeline. Time duration between `dataflow_read_timestamp` and `write_timestamp`.     |
| Replication lag total latency                 | Time duration between `source_timestamp` and `write_timestamp`.                                                                        |
| Invalid events                                | Number of events that were dropped because they have a schema incompatible with the spanner schema.                                  |
| Successful event retries                      | Number of events that were successfully read from dlq/retry and written to spanner.                                                    |
| Event retries                                 | Distribution of retries done for any event.                                                                                            |
| `spanner_writer_latency_ms`                   | Latency of creating and writing mutations from change events to spanner.                                                               |
| `transformation_latency_ms`                   | Latency of applying transformation to the events.                                                                                      |
| Dropped table exceptions                      | The events that were skipped from migration since the table was dropped from migration.                                                 |


### Metrics for retryDLQ run

| Metric Name                                   | Description                                                                                                                            |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Successful events                             | Total number of events successfully processed and applied to Spanner database                                                          |
| `elementsReconsumedFromDeadLetterQueue`       | The total number of events consumed from DLQ for retry                                                                                 |
| Elements requeued for retry                   | The total number of events that were re-queued for retry                                                                               |
| Conversion errors                             | Number of events that could not be converted to Spanner. This is a permanent error category.                                           |
| Skipped events                                | Number of events skipped from being written to spanner because the events were stale.                                                  |
| Other permanent errors                        | The remaining permanent errors.                                                                                                        |
| Transformed events                            | The number of events that were successfully transformed, including retries and permanent errors.                                       |
| Filtered events                               | The number of events that were filtered as a part of custom transformation.                                                          |
| Custom Transformation Exceptions              | The number of events that were errored out due to some exception in custom transformation jar.                                         |
| Total events processed                        | The number of events that were tried for forward migration, including retries and permanent errors.                                    |
| `apply_custom_transformation_impl_latency_ms` | Latency of applying custom transformation to the event.                                                                              |
| Replication lag system latency                | Time taken from events being read by Datastream to being written to Cloud Spanner. Time duration between `datastream_read_timestamp` and `write_timestamp` |
| Replication lag dataflow latency              | Time taken for events to get processed by the Dataflow pipeline. Time duration between `dataflow_read_timestamp` and `write_timestamp`.     |
| Replication lag total latency                 | Time duration between `source_timestamp` and `write_timestamp`.                                                                        |
| Invalid events                                | Number of events that were dropped because they have a schema incompatible with the spanner schema.                                  |
| Successful event retries                      | Number of events that were successfully read from dlq/retry and written to spanner.                                                    |
| Event retries                                 | Distribution of retries done for any event.                                                                                            |
| `spanner_writer_latency_ms`                   | Latency of creating and writing mutations from change events to spanner.                                                               |
| `transformation_latency_ms`                   | Latency of applying transformation to the events.                                                                                      |
| Dropped table exceptions                      | The events that were skipped from migration since the table was dropped from migration.                                                 |

It can happen that in retryDLQ mode, there are still permanent errors. To identify that all the retryable errors have been processed and only permanent errors remain for reprocessing - one can look at the ‘Successful events' count - it would remain constant after every retry iteration. Each retry iteration, the ‘elementsReconsumedFromDeadLetterQueue' would increment.

{: .note }

Dataflow metrics are approximate. In the event that there is Dataflow worker restart, the same set of events might be reprocessed and the counters may reflect excess/lower values. In such scenarios, it is possible that counters like *Successful events* might have values greater than the number of records written to Spanner.Similarly, it is possible that the *Retryable errors* is negative since the same retry record got successfully processed by different workers.

### Re-run commands

#### To rerun regular flow

To rerun the regular flow, the same command as original needs to be fired. Note: This will only work when not using the PubSub subscriptions for GCS files.The processing starts all over again, meaning the same Datastream outputs get reprocessed.

```
gcloud dataflow flex-template run <jobName> \
 --project=<project-name> --region=<region-name> \
 --template-file-gcs-location=gs://dataflow-templates-southamerica-west1/2023-09-12-00_RC00/flex/Cloud_Datastream_to_Spanner \
 --num-workers 1 --max-workers 50 \
 --enable-streaming-engine \
 --parameters databaseId=<database id>,deadLetterQueueDirectory=<GCS location of the DLQ directory>,gcsPubSubSubscription=<pubsub subscription being used in a gcs notification policy>,dlqGcsPubSubSubscription=<pubsub subscription being used in a dlq gcs notification policy>,instanceId=<spanner-instance-id>,sessionFilePath=<GCS location of the session json>,streamName=<data stream name>,transformationContextFilePath=<path to transformation context json>

```

These job parameters can be taken from the original job.

#### To re-run for reprocessing DLQ directory

This will reprocess the records marked as ‘severe' error records from the DLQ.  
Before running the Dataflow job, check if the main Dataflow job has non-zero retryable error count. In case there are referential error records - check that the dependent table data is populated completely from the source database.

Sample command to run the Dataflow job in retryDLQ mode is

```sh
gcloud  dataflow flex-template run <jobname> \
--region=<the region where the dataflow job must run> \
--template-file-gcs-location=gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner \
--additional-experiments=use_runner_v2 \
--parameters gcsPubSubSubscription=<pubsub subscription being used in a gcs notification policy>,streamName=<Datastream name>, \
instanceId=<Spanner Instance Id>,databaseId=<Spanner Database Id>,sessionFilePath=<GCS path to session file>, \
dlqGcsPubSubSubscription=<pubsub subscription being used in a dlq gcs notification policy>, \
deadLetterQueueDirectory=<GCS path to the DLQ>,runMode=retryDLQ
```

The following parameters can be taken from the regular forward migration Dataflow job:

```sh
region
gcsPubSubSubscription
streamName
instanceId
databaseId
sessionFilePath
deadLetterQueueDirectory
dlqGcsPubSubSubscription
```


#### Alternative: Retrying Severe Errors via the Regular Mode Pipeline
Instead of using the runMode=retryDLQ, you can re-process files from the severe directory using the currently running Regular Mode pipeline. 

**Important Note:** If you have a large number of entries in the DLQ, running the standard retryDLQ mode might lead to Out of Memory (OOM) errors in the pipeline. To handle this, you can use this retrial method **along with Pub/Sub approach by passing dlqGcsPubSubSubscription parameter.**


**Steps:**
1. Ensure pipeline is running in regular mode. If not, restart the pipeline in regular mode.
2. Resolve Issues: Address the underlying cause of the errors in the files located within gs://deadLetterQueueDirectory/severe/.
3. Move Files to Retry: Gradually move the files you want to reprocess from the severe directory to the retry directory.

Command to move a single file
```sh
gsutil mv gs://<bucket-name>/<dlq-path>/severe/failed-file-01.json gs://<bucket-name>/<dlq-path>/retry/
```
Command to move all files
```sh
gsutil -m mv gs://<bucket-name>/<dlq-path>/severe/* gs://<bucket-name>/<dlq-path>/retry/
```
4. Outcome:
- If a file is processed successfully, it is fully handled.
- If a file fails processing again, the standard Regular Mode retry logic applies. The event will be retried up to the configured maxRetries attempts within the retry mechanism or till a severe failure occurs.
- If the file still fails after all retries are exhausted in Regular Mode, the pipeline will move it back to the gs://deadLetterQueueDirectory/severe/ directory.
