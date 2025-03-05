---
layout: default
title: Issues and Suggestions
parent: Schema Conversion Workspace
grand_parent: SMT UI
nav_order: 3
---

# Issues and Suggestions
{: .no_toc }

Spanner migration tool scans through the generated spanner schema and notifies the user of any warnings or errors encountered. It also makes intelligent suggestions to the user which would help them utilize the spanner capabilities to the fullest.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Warnings

- Detection of an auto-increment key in source database because auto-increments are currently not supported for all source databases.
- Spanner data type consuming more storage than source data type
- Redundant indexes
- Addition of [synthetic primary key](../ui.md/#termsterminology) - synth_id
- [Hotspotting](https://cloud.google.com/spanner/docs/schema-design) due to timestamp or auto-increment keys
- Auto Increment has been converted to Sequence, set Ignore Range or Start with Counter to avoid duplicate value errors
- Dropping unsupported default values during schema migration.
- Detection of invalid default values in spanner added during column modification.
- Dropping of unsupported check constraints in spanner.
- Dropping check constraints if there is a data type mismatch in the referenced column.
- Dropping check constraints if there is referenced column in check constraints not found.
- Dropping check constraints if there is function referenced in check constraints is not found.

![](https://services.google.com/fh/files/misc/cc7.png)

## Errors

- Detection of unsupported check constraints in spanner.
- Data type mismatch of referenced column in check constraints.
- Referenced column in check constraints not found.
- Function referenced in check constraints is not found.

![](https://services.google.com/fh/files/misc/cc8.png)

## Suggestions

- Modifications related to converting a table into an interleaved one
- Converting an index to interleaved index

![](https://services.google.com/fh/files/helpcenter/asset-spnu1lr86ts.png)

![](https://services.google.com/fh/files/helpcenter/asset-3xj2ro46b6a.png)