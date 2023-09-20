---
layout: default
title: Individual Table migration
parent: Schema Conversion Workspace
grand_parent: SMT UI
nav_order: 5
---

# Individual Table migrations
{: .no_toc }

Spanner migration tool provides the ability to migrate a subset of tables from the source database to Spanner. This can help in a number of ways such as:

1. Running a POC on a sample set of tables.
2. Breaking up a monolithic database schema into multiple logical databases on Spanner.
3. Avoid migration of redundant/deprecated tables from source to Spanner.

Spanner migration tool supports skipping/restoring both, each table individually, and several tables in bulk.
In the section below, we will look at how this can be configured using the Spanner migration tool UI.

{: .highlight }
Note: For Postgres minimal downtime migrations, Spanner migration tool currently does not support configuring Datastream to stream only the selected tables. All tables will be streamed, but only the selected tables will be copied into Spanner.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Skipping/restoring individual tables

The schema conversion workspace lists all the tables read by Spanner migration tool from the source database. In order to skip/restore a table individually, do the following:

1. Click on the table to be skipped/restored on the left pane.
2. In the middle pane, next to the name of the table, click on `SKIP TABLE` or `RESTORE TABLE` button.
3. Enter the name of the table in the confirmation dialog.
4. Click confirm to skip/restore the table.

![](https://services.google.com/fh/files/helpcenter/asset-fajgvy8szur.png)

![](https://services.google.com/fh/files/helpcenter/asset-xh794zlmncd.png)

## Skipping/restoring tables in bulk

The schema conversion workspace lists all the tables read by Spanner migration tool from the source database. In order to skip/restore multiple tables at once, do the following:

1. Navigate to `Spanner draft` on the left-pane.
1. Click on the check-boxes of the tables to be skipped/restored.
2. Once all the candidate tables have been selected, click on the `SKIP` or `RESTORE` button placed at the top of the left pane. `SKIP` or `RESTORE` buttons will be enabled on the basis of the current state of the selected table. For example, if you selected three unskipped tables, only the `SKIP` button would be enabled.
3. A confirmation dialog will show the number of tables to be skipped/restored. Enter `SKIP` or `RESTORE` to confirm as per the selected operation. Note that if you have selected a combination of skipped and restored tables, state change will only be applied to qualifying tables.
4. Click confirm to skip/restore the selected tables.


![](https://services.google.com/fh/files/helpcenter/asset-xghjiq5wbhc.png)