---
layout: default
title: Schema Modification Features
parent: Schema Conversion Workspace
grand_parent: SMT UI
nav_order: 2
---

# Schema modification features
{: .no_toc }

When migrating a schema to Spanner, using the default conversion may be sub-optimal owing to functional differences in the features supported and different performance characteristics of Spanner. Following the [best practices](https://cloud.google.com/spanner/docs/schema-design), it may be better to change keys, drop or add some indexes, or add/remove columns to existing tables in the Spanner schema.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Tables

Users can view detailed information for a table by selecting it from the **Spanner Draft** section. This includes details on columns, primary keys, foreign keys, interleave property, spanner DDL and the option to edit these along with deleting or restoring the table.

### Column

Column tab provides information on the columns that are a part of the selected table. It also provides the option to edit the column wherein a user can modify a column name, delete a column, change the data type of the column, add auto-generation to the column or modify the null property of the column. Once the user is done with required modifications, they can click on **SAVE & CONVERT **and the update would reflect in the session file and across all the components in the database.

![](https://services.google.com/fh/files/misc/dv1.png)

#### Add Column

In addition to editing the existing columns in the Spanner draft mapped from the source database, users can also add new columns to the selected table.

![](https://services.google.com/fh/files/misc/add_column.png)
![](https://services.google.com/fh/files/misc/add_column_form.png)
![](https://services.google.com/fh/files/misc/new_column.png)

To specify custom transformation logic in the dataflow pipeline to populate these columns, please refer to the [Custom Transformation](../../transformations/CustomTransformation.md) section.

### Primary Key

Users can view and edit the primary key of a table from the primary key tab. They can remove/add a column from the primary key or change the order of columns in the primary key. Once these changes are made, the session file is updated and they can also be verified from the [SQL tab](#sql).  

![](https://services.google.com/fh/files/misc/migration-pk.png)

### Foreign Key

Users can view and edit the foreign key of a table from the foreign key tab. They can modify the foreign key constraint name, drop the foreign key or convert the foreign key into interleave, if the table is interleavable. Once these changes are made the [session file](../ui.md/#termsterminology) is updated.

![](https://services.google.com/fh/files/helpcenter/asset-2tfzryfujfp.png)

### Interleave

Interleaving physically co-locates child rows with parent rows in storage. Co-location can significantly improve performance.For example, if there is a _Customers_ table and an _Invoices_ table, and the application frequently fetches all the invoices for a customer, users can define Invoices as an interleaved child table of Customers. In doing so, a data locality relationship between two independent tables is declared resulting in significant performance improvement.
Spanner Migration Tool provides the option to convert a table into an interleaved table if it fulfills all the criteria.The Interleave tab shows up only for tables which are possible candidates for interleaving, based on the existing foreign keys. Once a table is converted into an interleaved table, the UI shows the information of the parent table. Users can also choose to remove this interleaving property and restore the foreign key by clicking on ‘**Convert back to foreign key**'.  

{: .note }
Interleaving property needs to be set during the migration and a table cannot be interleaved after migration.

![](https://services.google.com/fh/files/helpcenter/asset-jni7ugajpw.png)


#### Interleave any table on any table

Interleaving one table within another without Foreign key relationship is not currently supported on the Spanner migration tool UI. However, there is a workaround that customers can use to achieve this.

##### Prerequisites

To interleave a `child table` within a `parent table`, the following conditions must be met:

1. The column names in the child table that reference the parent table must exactly match the column names in the parent table. The columns must also have the same data type and constraints (e.g., NOT NULL).
2. The child table must reference all of the primary key columns from the parent table, and these columns must also be part of the primary key in the child table.
3. The column order for the referenced columns in the primary key of the child table must match the column order of the primary key in the parent table. Additionally, the referenced columns must appear at the beginning of the primary key set in the child table.

##### Procedure

1. Download the session file from the Spanner migration tool after connecting to the source database.
2. Locate the table ID of the parent table in the session file.
3. In the spSchema object, within the child table's section, set the Id field in the parentTable object to the parent table's ID. Also, specify the desired OnDelete action in the OnDelete field (NO ACTION or CASCADE), as illustrated below.

![](https://services.google.com/fh/files/misc/smt_session_interleaving_parent.png)
![](https://services.google.com/fh/files/misc/smt_session_interleaving_child.png)

4. Use the modified session file in the [SMT commands](../../cli/cli.md) or it can be imported via [SMT UI](../connect-source.md#load-session-file) and proceed further.

### Check Constraints
Users have the ability to view and modify check constraints of a table via the check constraints tab. They can alter the check constraint's name, condition, and even remove the check constraint entirely. Once these changes are made the [session file](../ui.md/#termsterminology) is updated.

![](https://services.google.com/fh/files/misc/cc3.png)

#### Add or Edit Constraint
Besides modifying the existing check constraint in the Spanner draft mapped from the source database, users can also add new constraints to the selected table.

![](https://services.google.com/fh/files/misc/cc2.png)

![](https://services.google.com/fh/files/misc/cc1.png)

#### Remove Constraint
In addition to adding check constraints, users can also remove the check constraints in the spanner draft for the selected table.

![](https://services.google.com/fh/files/misc/cc4.png)

### SQL

Once the user is done with all the schema modifications they can then visit the SQL tab which shows the Spanner DDL for the modified schema.

![](https://services.google.com/fh/files/helpcenter/asset-tl65unqu1yp.png)

### Drop & Restore Table

Spanner Migration Tool also provides the users with the capability to drop and restore tables from the spanner database. Once the table is dropped, it appears as **deleted** under the Spanner Draft section and can be **restored** from there.

![](https://services.google.com/fh/files/helpcenter/asset-ipyr4k3bttk.png)

![](https://services.google.com/fh/files/helpcenter/asset-0219n5c1npnz.png)

## Indexes

Spanner Migration Tool provides information on indexes for source and spanner databases. It gives details of columns that are a part of the index along with their sort order. Users can edit the index and modify the order of columns in the index,sort order of the columns, add new columns to the index and remove columns from the index.

![](https://services.google.com/fh/files/helpcenter/asset-tlaxupk31pe.png)

### Drop & Restore Index

Spanner Migration Tool also provides the users with the capability to drop and restore existing indexes from the spanner database. Once the index is deleted, it appears as **deleted** under the Spanner Draft section and can be **restored** from there.

### Add Secondary Index

Apart from the existing indexes for the source database, users can also add secondary indexes for any table, if required. In order to add an index for a table, the user needs to select the **Add Index** option and provide some details mandatory to create an index like **index name**, **columns** that are a part of the index and their **sort order**.

![](https://services.google.com/fh/files/helpcenter/asset-6xbpmfyeplw.png)

![](https://services.google.com/fh/files/helpcenter/asset-ck63akvjank.png)

## Auto-Generated Columns
*Only Supported for source database MySQL*

Auto-Generated Columns populate Spanner columns automatically if no value is provided. Currently Spanner Migration Tool support the following techniques for auto-generation:
1. UUID function: Generate a UUID (v4) as part of a table’s primary key DEFAULT expression.
2. Bit reverse function: Map existing integer keys using the same logic as a bit-reversed sequence to avoid hotspotting.

Refer to [documentation](https://cloud.google.com/spanner/docs/primary-key-default-value).

### UUID

The default recommendation for primary keys in Spanner is to use a Universally Unique Identifier, or UUID. Users can convert existing columns to be filled by UUID by choosing the **Edit** option in a table and under the **Auto-Generated** column choosing UUID.
![](https://services.google.com/fh/files/misc/column-edit-uuid.png)

### Sequences

Spanner offers a SEQUENCE object that generates unique integers as part of a primary key DEFAULT expression. However, unlike a monotonic sequence, the values generated by a Spanner sequence are distributed uniformly and thus won’t hotspot at scale. Existing **Auto-Increment** columns will be mapped to a new Sequence.

#### Create a new Sequence
In order to add a sequence, the user needs to select the **Add Sequence** option and provide some details mandatory to create a sequence like **sequence name** and **sequence type**. An existing sequence can also be modified using the **Edit** button.

<img src="https://services.google.com/fh/files/misc/sidebar-add-seq.png" width="30%" height="30%">

![](https://services.google.com/fh/files/misc/add-seq-column-info.png)
<img src="https://services.google.com/fh/files/misc/add-seq.png" width="30%" height="30%">

To assign a sequence to a column, users need to select the **Edit** option in a table and under the **Auto-Generated** column choose a Sequence.

![](https://services.google.com/fh/files/misc/edit-seq.png)

#### Drop a Sequence
Users can drop a sequence by selecting the sequence and clicking on **DROP SEQUENCE**. Once a sequence is dropped, all columns that used the sequence for auto-generated will have their auto-generation policy set to none.

![](https://services.google.com/fh/files/misc/seq-info.png)