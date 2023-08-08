---
layout: default
title: SMT Output(s)
nav_order: 9
description: "Artifacts generated when you run Spanner migration tool"
---

# Spanner migration tool artifacts
{: .no_toc }

Spanner migration tool generates several files as it runs. Each of the file have their own function, from storing an existing migration state to generating a report for the schema analyzed by it. Below is a description of each.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## File descriptions

### Schema file (ending in `schema.txt`)

Contains the generated Spanner schema, interspersed with comments that cross-reference to the relevant source schema definitions.

### Session file (ending in `session.json`)

Contains all schema and data conversion state endcoded as JSON. It is basically a snapshot of the session.

### Structured Report file (ending in `structured_report.json`)

Contains a JSON based structured analysis of the source to Spanner migration. The structured report can be used to in-depth analysis of Spanner migration tool findings via BI tools.

### Text Report file (ending in `report.txt`)

Contains a detailed analysis of the source to Spanner migration, including table-by-table stats and an analysis of Source types that don't cleanly map onto Spanner types. Note that source types that don't have a corresponding Spanner type are mapped to STRING(MAX).

### Bad data file (ending in `dropped.txt`)

{: .highlight }
This is only generated for [POC migrations](./poc/poc.md).

Contains details of data that could not be converted and written to Spanner, including sample bad-data rows. If there is no bad-data, this file is not written (and we delete any existing file with the same name from a previous run).

{: .note }
By default, these files are prefixed by the name of the Spanner database (with a
dot separator). The file prefix can be overridden using the `-prefix`
[option](#options).

## Spanner migration tool report

A Spanner migration tool report consists of elements defined below. A sample structured report can be found [here](https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/test_data/mysql_structured_report.json).

### Summary

Defines the overall quality of the conversion perfomed by the Spanner migration tool along with a rating.

### Migration Type

Defines the type of conversion performed by the Spanner migration tool. It is one of SCHEMA, DATA and SCHEMA_AND_DATA.

### Ignored Statements

Defines the statements in the source schema which have been ignored by the Spanner migration tool. For example, View related statements are currently ignored by the Spanner migration tool.

### Conversion Metdata

Defines the total time taken to perform the conversion. This may include other conversion related metadata in the future.

### Statement Stats

Statistics on different types of statements identified by the Spanner migration tool. This is only populated when processing dump files.

### Name Changes

Renaming related changes done by the Spanner migration tool to ensure Cloud Spanner compatibility.

### Individual Table Reports

Detailed table-by-table analysis showing how many columns were converted perfectly, with warnings etc.

### Unexpected Conditions

Unexpected conditions encountered by the Spanner migration tool while processing the source schema/data.
