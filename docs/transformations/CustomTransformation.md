---
layout: default
title: Custom Transformations
nav_order: 8
has_children: true
permalink: /custom-transformation
---

# Custom transformation
{: .no_toc }

Spanner migration tool can be used to perform minimal downtime migration for PostgreSQL using the GUI or the CLI.

{: .highlight }
Following instructions assume you have setup SMT by following the instructions in the [installation](../install.md) guide.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

Implementing a custom transformation

- The dataflow pipeline processes elements record by record and will call the custom transformation method(toSourceRow/toSpannerRow) once per element.
- not all column values need to be returned, only the ones returned will be updated. The rest will be migrated as normal
- the user can also add newer columns not there in source (but there on spanner).


Unit testing

Error handling

Monitoring
- Exceptions
- Latency
- Filtered event count
- Filtered events in GCS

Best practices
-  Time consuming operations in custom JAR can slow down the pipeline, since its executed at a per element level
- Account for retries, idempotency

Building a JAR
1. Checkout the dataflow code from github
2. 

Parameter details

Java object type mappings


-- Under the minimal downtime section, mention only how it needs to be used in the live migration flow. Actually it might not be required and only add a screenshot and gcloud command in the UI flow.