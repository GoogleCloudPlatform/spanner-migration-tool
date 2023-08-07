---
layout: default
title: MySQL
parent: Schema migration
nav_order: 2
---

# Schema migration for MySQL
{: .no_toc }

Spanner migration tool makes some assumptions while performing data type conversion from MySQL to Spanner.
There are also nuances to handling certain specific data types. These are captured below.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Data Type Mapping

The Spanner migration tool maps MySQL types to Spanner types as follows:

| MySQL Type                                        | Spanner Type    | Notes                           |
|---------------------------------------------------|-----------------|---------------------------------|
| `BOOL`, `BOOLEAN`,<br/>`TINYINT(1)`               | `BOOL`          |                                 |
| `BIGINT`                                          | `INT64`         |                                 |
| `BINARY`, `VARBINARY`                             | `BYTES(MAX)`    |                                 |
| `BLOB`, `MEDIUMBLOB`,<br/>`TINYBLOB`, `LONGBLOB`  | `BYTES(MAX)`    |                                 |
| `BIT`                                             | `BYTES(MAX)`    |                                 |
| `CHAR`                                            | `STRING(1)`     | CHAR defaults to length 1       |
| `CHAR(N)`                                         | `STRING(N)`     | c                               |
| `DATE`                                            | `DATE`          |                                 |
| `DATETIME`                                        | `TIMESTAMP`     | t                               |
| `DECIMAL`, `NUMERIC`                              | `NUMERIC`       | p                               |
| `DOUBLE`                                          | `FLOAT64`       |                                 |
| `ENUM`                                            | `STRING(MAX)`   |                                 |
| `FLOAT`                                           | `FLOAT64`       | s                               |
| `INTEGER`, `MEDIUMINT`,<br/>`TINYINT`, `SMALLINT` | `INT64`         | s                               |
| `JSON`                                            | `JSON`          |                                 |
| `SET`                                             | `ARRAY<STRING>` | SET only supports string values |
| `TEXT`, `MEDIUMTEXT`,<br/>`TINYTEXT`, `LONGTEXT`  | `STRING(MAX)`   |                                 |
| `TIMESTAMP`                                       | `TIMESTAMP`     |                                 |
| `VARCHAR`                                         | `STRING(MAX)`   |                                 |
| `VARCHAR(N)`                                      | `STRING(N)`     | c                               |

Spanner does not support `spatial` datatypes of MySQL. Along with `spatial`
datatypes, all other types map to `STRING(MAX)`. Some of the mappings in this
table represent potential changes of precision (marked p), differences in
treatment of timezones (marked t), differences in treatment of fixed-length
character types (marked c), and changes in storage size (marked s). We discuss
these, as well as other limits and notes on schema conversion, in the following
sections.

## DECIMAL and NUMERIC

[Spanner's NUMERIC
type](https://cloud.google.com/spanner/docs/data-types#decimal_type) can store
up to 29 digits before the decimal point and up to 9 after the decimal point.
MySQL's NUMERIC type can potentially support higher precision than this, so
please verify that Spanner's NUMERIC support meets your application needs.  Note
that in MySQL, NUMERIC is implemented as DECIMAL, so the remarks about DECIMAL
apply equally to NUMERIC.

## TIMESTAMP and DATETIME

MySQL has two timestamp types: `TIMESTAMP` and `DATETIME`. Both provide
microsecond resolution, but neither actually stores a timezone with the data.
The key difference between the two types is that MySQL converts `TIMESTAMP` values
from the current time zone to UTC for storage, and back from UTC to the current time
zone for retrieval. This does not occur for `DATETIME` and data is returned without a
timezone. For `TIMESTAMP`, timezone can be set by time zone offset parameter.

Spanner has a single timestamp type. Data is stored as UTC (there is no separate
timezone) Spanner client libraries convert timestamps to UTC before sending them
to Spanner. Data is always returned as UTC. Spanner's timestamp type is
essentially the same as `TIMESTAMP`, except that there is no analog of
MySQL's timezone offset parameter.

In other words, mapping MySQL `DATETIME` to `TIMESTAMP` is fairly
straightforward, but care should be taken with MySQL `DATETIME` data
because Spanner clients will not drop the timezone.

## CHAR(n) and VARCHAR(n)

The semantics of fixed-length character types differ between MySQL and
Spanner. The `CHAR(n)` type in MySQL is right-padded with spaces. If a string
value smaller than the limit is stored, spaces will be added to pad it out to
the specified length. If a string longer than the specified length is stored,
and the extra characters are all spaces, then it will be silently
truncated. Moreover, trailing spaces are ignored when comparing two values. In
constrast, Spanner does not give special treatment to spaces, and the specified
length simply represents the maximum length that can be stored. This is close to
the semantics of MySQL's `VARCHAR(n)`. However there are some minor
differences. For example, even `VARCHAR(n)` has some special treatment of
spaces: string with trailing spaces in excess of the column length are truncated
prior to insertion and a warning is generated.

## SET

MySQL `SET` is a string object that can hold muliple values, each of which must be
chosen from a list of permitted values specified when the table is created. `SET`
is being mapped to Spanner type `ARRAY<STRING>`. Validation of `SET` element values
will be dropped in Spanner. Thus for production use, validation needs to be done
in the application.

## Spatial datatypes

MySQL spatial datatypes are used to represent geographic feature.
It includes `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTIPOLYGON`
and `GEOMETRYCOLLECTION` datatypes. Spanner does not support spatial data types.
This datatype are currently mapped to standard `STRING` Spanner datatype.

## Storage Use

The tool maps several MySQL types to Spanner types that use more storage.
For example, `SMALLINT` is a two-byte integer, but it maps to Spanner's `INT64`,
an eight-byte integer.

## Primary Keys

Spanner requires primary keys for all tables. MySQL recommends the use of
primary keys for all tables, but does not enforce this. When converting a table
without a primary key, Spanner migration tool will create a new primary key of type
INT64. By default, the name of the new column is `synth_id`. If there is already
a column with that name, then a variation is used to avoid collisions.

## NOT NULL Constraints

The tool preserves `NOT NULL` constraints. Note that Spanner does not require
primary key columns to be `NOT NULL`. However, in MySQL, a primary key is a
combination of `NOT NULL` and `UNIQUE`, and so primary key columns from
MySQL will be mapped to Spanner columns that are both primary keys and `NOT NULL`.

## Foreign Keys

The tool maps MySQL foreign key constraints into Spanner foreign key constraints, and
preserves constraint names where possible. Since Spanner doesn't support `ON DELETE`
and `ON UPDATE` actions, we drop them.

## Default Values

Spanner does not currently support default values. We drop these
MySQL features during conversion.

## Secondary Indexes

The tool maps MySQL secondary indexes to Spanner secondary indexes, and preserves
constraint names where possible. Note that Spanner requires index key constraint
names to be globally unique (within a database), but in MySQL they only have to be
unique for a table, so we add a uniqueness suffix to a name if needed. The tool also
maps `UNIQUE` constraint into `UNIQUE` secondary index. Note that due to limitations of our
mysqldump parser, we are not able to handle key column ordering (i.e. ASC/DESC) in
mysqldump files. All key columns in mysqldump files will be treated as ASC.

## Other MySQL features

MySQL has many other features we haven't discussed, including functions,
sequences, procedures, triggers, (non-primary) indexes and views. The tool does
not support these and the relevant statements are dropped during schema
conversion.

See [Migrating from MySQL to Cloud Spanner](https://cloud.google.com/solutions/migrating-mysql-to-spanner)
for a general discussion of MySQL to Spanner migration issues.
Spanner migration tool follows most of the recommendations in that guide. The main
difference is that we map a few more types to `STRING(MAX)`.
