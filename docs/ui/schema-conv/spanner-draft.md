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

Column tab provides information on the columns that are a part of the selected table. It also provides the option to edit the column wherein a user can modify a column name, delete a column, change the data type of the column or modify the null property of the column. Once the user is done with required modifications, they can click on **SAVE & CONVERT **and the update would reflect in the session file and across all the components in the database.

![](https://services.google.com/fh/files/helpcenter/asset-oo8ugjhmgd.png)

### Primary Key

Users can view and edit the primary key of a table from the primary key tab. They can remove/add a column from the primary key or change the order of columns in the primary key. Once these changes are made, the session file is updated and they can also be verified from the [SQL tab](#sql).  

![](https://services.google.com/fh/files/helpcenter/asset-syrsuek8k4r.png)

### Foreign Key

Users can view and edit the foreign key of a table from the foreign key tab. They can modify the foreign key constraint name, drop the foreign key or convert the foreign key into interleave, if the table is interleavable. Once these changes are made the [session file](../ui.md/#termsterminology) is updated.

![](https://services.google.com/fh/files/helpcenter/asset-2tfzryfujfp.png)

### Interleave

Interleaving physically co-locates child rows with parent rows in storage. Co-location can significantly improve performance.For example, if there is a _Customers_ table and an _Invoices_ table, and the application frequently fetches all the invoices for a customer, users can define Invoices as an interleaved child table of Customers. In doing so, a data locality relationship between two independent tables is declared resulting in significant performance improvement.
Spanner Migration Tool provides the option to convert a table into an interleaved table if it fulfills all the criteria.The Interleave tab shows up only for tables which are possible candidates for interleaving, based on the existing foreign keys. Once a table is converted into an interleaved table, the UI shows the information of the parent table. Users can also choose to remove this interleaving property and restore the foreign key by clicking on ‘**Convert back to foreign key**'.  

{: .note }
Interleaving property needs to be set during the migration and a table cannot be interleaved after migration.

![](https://services.google.com/fh/files/helpcenter/asset-jni7ugajpw.png)

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