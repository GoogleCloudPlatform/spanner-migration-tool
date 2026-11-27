---
layout: default
title: SQL Server
parent: Data Type Conversion
nav_order: 5
---

# Schema migration for SQL Server
{: .no_toc }

Spanner migration tool makes some assumptions while performing data type conversion from SQL Server to Spanner.
There are also nuances to handling certain specific data types. These are captured below.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Data Type Mapping

The Spanner migration tool maps SQL Server types to Spanner types as follows. The last
column shows the warning, if any, that the conversion report shows for a column of that type.

| **SQL Server Type** | **Spanner Type (GoogleSQL)** | **Spanner Type (PostgreSQL dialect)** | **Report** |
|:---|:---|:---|:---|
| `BIGINT` | `INT64` | `bigint` | |
| `INT`, `SMALLINT`, `TINYINT` | `INT64` | `bigint` | uses more storage, see [Storage Use](#storage-use) |
| `BIT` | `BOOL` | `boolean` | |
| `REAL` | `FLOAT32` | `real` | |
| `FLOAT` | `FLOAT64` | `double precision` | |
| `DECIMAL(p,0)`, `NUMERIC(p,0)` with p up to 18 | `INT64` | `bigint` | |
| other `DECIMAL(p,s)`, `NUMERIC(p,s)` | `NUMERIC` | `numeric` | see [DECIMAL and NUMERIC](#decimal-and-numeric) |
| `MONEY`, `SMALLMONEY` | `NUMERIC` | `numeric` | |
| `DATE` | `DATE` | `date` | |
| `TIME` | `STRING(MAX)` | `character varying` | Spanner has no time type |
| `DATETIME`, `DATETIME2`, `SMALLDATETIME` | `TIMESTAMP` | `timestamp with time zone` | the source type has no time zone |
| `DATETIMEOFFSET` | `TIMESTAMP` | `timestamp with time zone` | |
| `CHAR(n)`, `VARCHAR(n)`, `NCHAR(n)`, `NVARCHAR(n)` | `STRING(n)` | `character varying(n)` | |
| `VARCHAR(MAX)`, `NVARCHAR(MAX)` | `STRING(2621440)` | `character varying` | possible string overflow |
| `TEXT`, `NTEXT`, `XML` | `STRING(MAX)` | `character varying` | possible string overflow |
| `BINARY(n)`, `VARBINARY(n)` | `BYTES(n)` | `bytea` | |
| `VARBINARY(MAX)`, `IMAGE` | `BYTES(10485760)` | `bytea` | possible overflow, see [Binary types](#binary-types) |
| `ROWVERSION` (`TIMESTAMP`) | `BYTES(8)` | `bytea` | |
| `UNIQUEIDENTIFIER` | `UUID` | `uuid` | |
| `JSON` (SQL Server 2025) | `JSON` | `jsonb` | |
| `VECTOR` (SQL Server 2025) | `ARRAY<FLOAT64>` | `character varying` | array migration not fully supported |
| `GEOGRAPHY`, `GEOMETRY`, `HIERARCHYID`, `SQL_VARIANT` | `STRING(MAX)` | `character varying` | no appropriate Spanner type, see [Types with no Spanner equivalent](#types-with-no-spanner-equivalent) |

`SYSNAME` is reported by SQL Server as `NVARCHAR(128)` and is mapped like any other
`NVARCHAR(n)` column. Any type not listed above maps to `STRING(MAX)` (`character varying`)
with the "no appropriate Spanner type" warning.

## DECIMAL and NUMERIC

A `DECIMAL` or `NUMERIC` column with scale 0 and at most 18 digits (for example
`DECIMAL(18,0)`) is mapped to `INT64`, which holds every such value. Other `DECIMAL` and
`NUMERIC` columns are mapped to `NUMERIC`.

[Spanner's GoogleSQL NUMERIC type](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#decimal_type)
holds up to 29 digits before the decimal point and up to 9 after it. SQL Server allows up to
38 digits, so a column with more than 9 digits after the decimal point or more than 29 before
it (for example `DECIMAL(38,10)` or `DECIMAL(38,0)`) gets a warning that precision could be
lost. The PostgreSQL dialect's `numeric` has no such limit, so there is no warning there.

The PostgreSQL dialect does not support `numeric` primary key columns. A `DECIMAL` or
`NUMERIC` primary key column that is not mapped to `bigint` is mapped to `character varying`
instead, with a warning.

## Date and time types

`DATETIME`, `DATETIME2` and `SMALLDATETIME` store no time zone, while a Spanner
`TIMESTAMP` is an absolute point in time. The values are migrated as if they were in UTC, and
the report warns about the missing time zone (once per table, naming one example column).
Check how your application interprets these values.

`DATETIMEOFFSET` values are migrated as the same point in time. Spanner does not store the
offset, so the original offset of each value is not kept.

Spanner has no time-of-day type, so `TIME` is mapped to `STRING(MAX)` with a warning.

## Character types

`CHAR(n)`, `VARCHAR(n)`, `NCHAR(n)` and `NVARCHAR(n)` keep their length. `VARCHAR(MAX)` and
`NVARCHAR(MAX)` are mapped to `STRING(2621440)`, Spanner's maximum length, and `TEXT`,
`NTEXT` and `XML` to `STRING(MAX)`. These SQL Server types can hold more data than a Spanner
`STRING` value (2,621,440 characters), so the report warns about a possible string overflow
for each such column.

## Binary types

`VARBINARY(MAX)` and `IMAGE` are mapped to `BYTES(10485760)` in GoogleSQL and to `bytea` in
the PostgreSQL dialect. Spanner limits a `BYTES` value to 10 MiB
([Spanner limits](https://cloud.google.com/spanner/quotas#tables)), while these SQL Server
types hold up to 2 GB per value. The report carries a "Possible overflow in Spanner" warning
for each such column: check that your data fits.

`ROWVERSION` (and its synonym `TIMESTAMP`) is mapped to `BYTES(8)`, and its values are
migrated as their original 8 bytes.

## Types with no Spanner equivalent

`GEOGRAPHY`, `GEOMETRY`, `HIERARCHYID` and `SQL_VARIANT` have no Spanner equivalent and are
mapped to `STRING(MAX)` (`character varying`). The report warns about each such column, and
the column is made nullable in Spanner even if it is `NOT NULL` in SQL Server.

## Storage Use

The tool maps `INT`, `SMALLINT` and `TINYINT` to Spanner's `INT64`, an eight-byte integer, so
they use more storage than in SQL Server. The report shows this once per table, naming one
example column.

## Primary Keys

Spanner requires primary keys for all tables. When converting a table without a primary key,
Spanner migration tool uses the columns of the table's first unique index, if it has one, as
the primary key, and the report shows a warning. Otherwise it creates a new primary key column of type `STRING(50)`
(`character varying(50)` in the PostgreSQL dialect). By default, the name of the new column is
`synth_id`. If there is already a column with that name, then a variation is used to avoid
collisions.

## NOT NULL Constraints

The tool preserves `NOT NULL` constraints, except on columns of the types listed in
[Types with no Spanner equivalent](#types-with-no-spanner-equivalent). Primary key columns are
`NOT NULL` in SQL Server and stay `NOT NULL` in Spanner.

## Foreign Keys

The tool maps SQL Server foreign key constraints to Spanner foreign key constraints, and
preserves constraint names where possible. The `ON DELETE` and `ON UPDATE` actions of a
SQL Server foreign key are not migrated: the Spanner foreign key is created without actions,
and the report shows a warning for each table that has foreign keys. Foreign keys that are
disabled in SQL Server are migrated as enforced foreign keys.

## Check Constraints

The tool migrates `CHECK` constraints that are enabled in SQL Server. Spanner checks each
one, and any that it rejects is dropped with a warning in the report. Constraint names that
are not valid in Spanner are changed to valid names. `CHECK` constraints that are disabled
in SQL Server are not migrated.

## Secondary Indexes

The tool maps clustered and nonclustered indexes, other than the primary key, to Spanner
secondary indexes, keeping the key column order (`ASC`/`DESC`). Included columns become
`STORING` columns (`INCLUDE` in the PostgreSQL dialect), and `UNIQUE` constraints become
`UNIQUE` indexes. Spanner requires index names to be unique within a database, while
SQL Server only requires them to be unique within a table, so a suffix is added to a name if
needed.

Other index types (XML, spatial, columnstore and full-text indexes) are not migrated. The
filter of a filtered index is not migrated, so the Spanner index covers all rows. Indexes
that are disabled in SQL Server are migrated as normal indexes.

## Identity Columns

The tool maps SQL Server `IDENTITY` columns to [Spanner IDENTITY
columns](https://cloud.google.com/spanner/docs/primary-key-default-value#identity-columns)
(`GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)`). The seed and increment of the
SQL Server column are not used. Set the SKIP RANGE and/or START COUNTER WITH values to avoid
duplicate key errors, as described for
[MySQL auto-increment columns](mysql.md#auto-increment-columns).

## Computed Columns

The tool maps computed columns to Spanner generated columns. `PERSISTED` computed columns
become `STORED` generated columns, and other computed columns become virtual generated
columns.

## Default Values

The tool migrates `DEFAULT` values that Spanner accepts. Spanner checks each default; any
that it rejects is dropped, and the report notes this once per table, naming one example
column.

- Number and string literals are migrated as they are. `DEFAULT NULL` is treated as no
  default.
- Expressions, such as `(1+1)` or `UPPER('a')`, are migrated as expressions.
- `BIT` defaults (`0`, `1`, `'true'`, `'false'`) become `FALSE` or `TRUE`.
- The current-time functions `GETDATE()`, `SYSDATETIME()`, `GETUTCDATE()`,
  `SYSUTCDATETIME()` and `SYSDATETIMEOFFSET()` (SQL Server stores `CURRENT_TIMESTAMP` as
  `getdate()`) become `CURRENT_TIMESTAMP()` on columns mapped to `TIMESTAMP`, and
  `CURRENT_DATE()` on `DATE` columns. In the PostgreSQL dialect they become
  `CURRENT_TIMESTAMP` and `CURRENT_DATE`.

`CURRENT_DATE()` returns the date in the database's default time zone
(America/Los_Angeles unless the database sets another one), while `GETDATE()` and
`SYSDATETIME()` return the SQL Server machine's local date, so the default date can differ
around midnight. On `DATE` columns, `GETUTCDATE()` and `SYSUTCDATETIME()` become
`CURRENT_DATE('UTC')` in GoogleSQL. The PostgreSQL dialect has no way to set the time zone,
so they become `CURRENT_DATE` there.

The following defaults are not migrated: current-time functions on columns that are not
mapped to `TIMESTAMP` or `DATE` (for example `TIME` or `VARCHAR` columns), `NEWID()`, and
sequence defaults (`NEXT VALUE FOR`).

## Other SQL Server features

SQL Server has many other features we haven't discussed, including sequences, triggers,
views, stored procedures and functions. The tool does not migrate these.
