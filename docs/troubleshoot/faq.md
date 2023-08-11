---
layout: default
title: FAQs
parent: Troubleshooting
nav_order: 2
---

# FAQs
{: .no_toc }

This section gives information about some FAQs regarding Spanner migration tool.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

### How to start the HarbourBridge UI ?

To bring up the HarbourBridge UI, please follow the steps mentioned [here](../install.md/#installation-guide).

### Can HarbourBridge be used without connecting to the spanner instance?

Yes, HarbourBridge can be used for schema assessment and modifications without connecting to the spanner instance

### When is a table interleavable?

A table is interleavable into a parent table if it has a foreign key referencing the parent table and the primary key is a superset of the primary key of the parent table.

### What to do in case you are unable to connect to a spanner instance?

If you are connecting from a GCE VM please verify the access scope of your GCE VM. It should be set to **Allow full access to all Cloud APIs **to allow connections to Cloud API. In case it is set to **default** access, please modify the access level by following the steps below and try again -
    1. Stop the VM
    2. Edit the VM configuration and change the access scope to **Allow full access to all Cloud APIs.**
    3. Restart the VM

Otherwise, execute the following command: **gcloud auth application-default login**

### What happens behind the scenes in minimal downtime migration?

HarbourBridge orchestrates the entire process using a unified interface, which comprises the following steps:

1. Setting up a GCS bucket to store incoming change events on the source database while the snapshot migration progresses.
2. Setting up the bulk load of the snapshot data and stream of incoming change events using Datastream. **Within the HarbourBridge UI, source and target connection profile will need to be setup**
3. Setting up the Dataflow job to migrate the change events into Spanner, which drains the GCS bucket over time.

Once the GCS bucket is almost empty, users need to stop writing to the source database so that the remaining change events can be applied. This results in a short downtime while Spanner catches up to the source database. Afterwards,the application can be cut over to Spanner. Currently, HarbourBridge provides minimal downtime migration support for **MySQL, Oracle** **and PostgreSQL** databases.
