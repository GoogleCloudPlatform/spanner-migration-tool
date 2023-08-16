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

Points 1 to 4 above are retryable errors - the Dataflow job automatically retries them at intervals of 10 minutes for 500 times. In most cases, this should be good enough for the retryable records to succeed, however, even if after exhausting all the retries, these are not successful - then these records are marked as ‘severe' error category. Such ‘severe' errors can be retried later with a ‘retryDLQ' mode of the Dataflow job (discussed below in the ‘Retry command' section).  
The following scenarios results in skipping of records, they are not really errors:

1. Invalid structure of records read from Datastream output
1. Table that existed in source but was dropped during schema conversion

Note there can be exceptions like invalid arguments to the Dataflow pipeline - these cause the pipeline to halt.

## Metrics

Migration progress can be tracked by monitoring the Dataflow job and following custom metrics are exposed:

### Metrics for regular run

| Metric Name                           | Description                                                                                                                      |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| Successful events                     | Total number of events successfully processed and applied to Spanner database                                                    |
| Retryable errors                      | The count of events that were errored out but will be retried                                                                    |
| Total permanent errors                | The number of events that are errored out with non-retriable errors in addition to the number of errors after exhausting retries |
| Conversion errors                     | Number of events that could not be converted to Spanner.This is a permanent error category.                                      |
| Skipped events                        | The events that are skipped from migration since the table was dropped from migration                                            |
| Other permanent errors                | The remaining permanent errors.                                                                                                  |
| Total events processed                | The number of events that were tried for forward migration, including retries and permanent errors.                              |
| elementsReconsumedFromDeadLetterQueue | The total number of events consumed from DLQ for retry                                                                           |


### Metrics for retryDLQ run

| Metric Name                           | Description                                                                                         |
|---------------------------------------|-----------------------------------------------------------------------------------------------------|
| Successful events                     | Total number of events successfully processed and applied to Spanner database                       |
| elementsReconsumedFromDeadLetterQueue | The total number of events consumed from DLQ for retry                                              |
| Elements requeued for retry           | The total number of events that were re queued for retry                                            |
| Conversion errors                     | Number of events that could not be converted to Spanner.This is a permanent error category.         |
| Skipped events                        | The events that are skipped from migration since the table was dropped from migration               |
| Other permanent errors                | The remaining permanent errors.                                                                     |
| Total events processed                | The number of events that were tried for forward migration, including retries and permanent errors. |

It can happen that in retryDLQ mode, there are still permanent errors. To identify that all the retryable errors have been processed and only permanent errors remain for reprocessing - one can look at the ‘Successful events' count - it would remain constant after every retry iteration. Each retry iteration, the ‘elementsReconsumedFromDeadLetterQueue' would increment.

### Retry command

This will reprocess the records marked as ‘severe' error records from the DLQ.  
Before running the Dataflow job, check if the main Dataflow job has non-zero retryable error count. In case there are referential error records - check that the dependent table data is populated completely from the source database.

Sample command to run the Dataflow job in retryDLQ mode is

```sh
gcloud beta dataflow flex-template run <jobname> --region=<the region where the dataflow job must run> --template-file-gcs-location=<location of the template image specification json> --additional-experiments=use_runner_v2 --parameters inputFilePattern=<GCS location of the input file pattern>,streamName=<Datastream name>,instanceId=<Spanner Instance Id>,databaseId=<Spanner Database Id>,sessionFilePath=<GCS path to session file>,deadLetterQueueDirectory=<GCS path to the DLQ>,runMode=retryDLQ
```

The following parameters can be taken from the regular forward migration Dataflow job:

```sh
region
inputFilePattern
streamName
instanceId
databaseId
sessionFilePath
deadLetterQueueDirectory
```
