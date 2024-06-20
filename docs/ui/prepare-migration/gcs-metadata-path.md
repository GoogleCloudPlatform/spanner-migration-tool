---
layout: default
title: GCS Metadata Path (Optional)
parent: Prepare Migration Page
grand_parent: SMT UI
nav_order: 7
---

# GCS Metadata Path (Optional)
{: .no_toc }

{: .important }

In case of minimal downtime sharded migration, for each shard, the Datastream jobs launched by the Spanner Migration Tool write data to a destination path on Google Cloud Storage. To store the metadata information such as session file, a new GCS Bucket is hence created. If the user does not wish for a new GCS Bucket to be created or wishes to name the bucket created, then the **GCS Metadata Path** Parameter can be set **optionally**.

![](https://services.google.com/fh/files/misc/gcs-bucket-metadata.png)
