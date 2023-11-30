---
layout: default
title: Tune Cloud Storage (Optional)
parent: Prepare Migration Page
grand_parent: SMT UI
nav_order: 5
---

# Tune Cloud Storage (Optional)
{: .no_toc }

In case of minimal downtime migration, the Datastream jobs launched by the Spanner Migration Tool write data to a destination path on Google Cloud Storage. This data is the staging data dumped by Datastream from the source database and read by the Dataflow pipeline while writing to Spanner. The destination bucket can be **optionally** tuned via SMT. Currently, the only supported tuning is enabling ttl on the data outputted by Datastream.

## Tuning use cases

SMT by default does not add any configuration on the destination GCS bucket. 
With the help of tuning, one can enable:
- Enable a delete lifecycle rule on the data directory to delete objects after a certain age.


{: .highlight }

To tune GCS, first specify the target database in the 'Configure Spanner Database' step. This enables the configure button for the remaining steps.

![](https://services.google.com/fh/gumdrop/preview/misc/gcs-tuning.png)

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

### Set TTL
Specify the TTL in days to enable automatic deletion of datastream output records. By default, TTL is disabled. When enabled, SMT establishes a delete lifecycle rule on the data directory, which stores datastream output records. This rule automatically deletes objects once they reach the specified TTL.

{: .warning }

Setting TTL improperly can lead to completeness issues during migration if the data is deleted by the lifecycle policy prematurely.