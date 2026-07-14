---
layout: default
title: Conversion Assessment
parent: Schema Conversion Workspace
grand_parent: SMT UI
nav_order: 1
---

# Conversion Assessment
{: .no_toc }

Conversion assessment helps the user to understand the complexity of the schema conversion. It is broken down into 3 categories:
*   **Can be converted automatically**: No warnings or errors, and the table has a primary key.
*   **Requires minimal conversion changes**: Warnings are present but they affect less than 1/3 of the columns in the table, or there are no warnings but the primary key is missing.
*   **Requires high complexity conversion changes**: Warnings affect 1/3 or more of the columns in the table, or there are hard errors.

### How Complexity is Calculated

The tool calculates complexity based on the ratio of warnings to the total number of columns in a table. This means a table with fewer columns is more sensitive to warnings than a table with many columns.

For example, if two tables have the exact same 2 issues:
*   Table A has 4 columns: Ratio = 2/4 = 0.5 (>= 1/3). Status: **Requires high complexity conversion changes**.
*   Table B has 10 columns: Ratio = 2/10 = 0.2 (< 1/3). Status: **Requires minimal conversion changes**.


![](https://services.google.com/fh/files/helpcenter/conv_report.png)