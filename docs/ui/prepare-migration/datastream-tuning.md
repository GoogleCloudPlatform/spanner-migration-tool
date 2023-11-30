---
layout: default
title: Tune Datastream (Optional)
parent: Prepare Migration Page
grand_parent: SMT UI
nav_order: 4
---

# Tune Datastream (Optional)
{: .no_toc }

In case of minimal downtime migration, the Datastream jobs launched by the Spanner Migration Tool can be **optionally** tuned. Tuning refers to tweaking the default parameters to run Datastream is a custom configuration.

## Tuning use cases

SMT by default launches datastream 50 backfill tasks and 5 cdc tasks.

Some use cases when the user would want to tweak these:
- Reduce load during backfill on source database
- Support more table processing in parallel for change events


{: .highlight }

To tune datastream, first specify the target database in the 'Configure Spanner Database' step. This enables the configure button for the remaining steps.

![](https://services.google.com/fh/gumdrop/preview/misc/datastream-tuning.png)

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

{: .highlight }

SMT exposes the maxConcurrentBackfillTasks and maxConcurrentCdcTasks for tuning. Please reach out to us if you have a use-case that is not satisfied by the provided configurations.


Increasing the number of tasks does not speedup the processing of individual tables. Since each task is dedicated to processing a single table, adding more tasks will not enhance the processing speed of a single table. Essentially, the number of tasks determines the maximum number of tables that can be processed simultaneously.

### Max Concurrent Backfill Tasks
Specify the maximum parallel tasks during backfill. You can use values from 1 to 50 (inclusive). The default value is 50.

### Max Concurrent Cdc Tasks (Not available for Postgres source)
Specify the maximum parallel tasks during CDC. You can use values from 1 to 50 (inclusive). The default value is 5.
