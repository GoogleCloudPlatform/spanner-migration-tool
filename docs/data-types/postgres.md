---
layout: default
title: PostgreSQL
parent: Data Type Conversion
nav_order: 3
---

# Schema migration for PostgreSQL
{: .no_toc }

Spanner migration tool makes some assumptions while performing data type conversion from PostgreSQL to Spanner.
There are also nuances to handling certain specific data types. These are captured below.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Data type mapping

The Spanner migration tool maps PostgreSQL types to Spanner types as follows:

| PostgreSQL Type    | Spanner Type           | Notes                                                         |
|--------------------|------------------------|---------------------------------------------------------------|
| `BOOL`             | `BOOL`                 |                                                               |
| `BIGINT`           | `INT64`                |                                                               |
| `BIGSERIAL`        | `INT64`                | dropped autoincrement functionality                           |
| `BYTEA`            | `BYTES(MAX)`           |                                                               |
| `CHAR`             | `STRING(1)`            | CHAR defaults to length 1                                     |
| `CHAR(N)`          | `STRING(N)`            | differences in treatment of fixed-length character types      |
| `DATE`             | `DATE`                 |                                                               |
| `DOUBLE PRECISION` | `FLOAT64`              |                                                               |
| `INTEGER`          | `INT64`                | changes in storage size                                       |
| `NUMERIC`          | `NUMERIC`              | potential changes of precision                                |
| `REAL`             | `FLOAT64`              | changes in storage size                                       |
| `SERIAL`           | `INT64`                | dropped autoincrement functionality , changes in storage size |
| `SMALLINT`         | `INT64`                | changes in storage size                                       |
| `TEXT`             | `STRING(MAX)`          |                                                               |
| `TIMESTAMP`        | `TIMESTAMP`            | differences in treatment of timezones                         |
| `TIMESTAMPTZ`      | `TIMESTAMP`            |                                                               |
| `VARCHAR`          | `STRING(MAX)`          |                                                               |
| `VARCHAR(N)`       | `STRING(N)`            | differences in treatment of fixed-length character types      |
| `JSON`, `JSONB`    | `JSON`                 |                                                               |
| `ARRAY(`pgtype`)`  | `ARRAY(`spannertype`)` | if scalar type pgtype maps to spannertype                     |

All other types map to `STRING(MAX)`.

## NUMERIC

[Spanner's NUMERIC
type](https://cloud.google.com/spanner/docs/data-types#decimal_type) can store
up to 29 digits before the decimal point and up to 9 after the decimal point.
PostgreSQL's NUMERIC type can potentially support higher precision that this, so
please verify that Spanner's NUMERIC support meets your application needs.

## BIGSERIAL and SERIAL

Spanner does not support autoincrementing types, so these both map to `INT64`
and the autoincrementing functionality is dropped.

## TIMESTAMP

PosgreSQL has two timestamp types: `TIMESTAMP` and `TIMESTAMPTZ`. Both have an 8
byte data representation and provide microsecond resolution, but neither
actually stores a timezone with the data. The key difference between the two
types is how string literals are converted to timestamps and queries return
data. For `TIMESTAMP`, all timezone information is dropped, and data is returned
without a timezone. For `TIMESTAMPTZ`, string literals are converted to UTC,
using the literal's timezone if it is specified, or the PostgreSQL's timezone
paramater if not. When data is printed stored data (in UTC) is converted to the
timezone from the timezone parameter

Spanner has a single timestamp type. Data is stored as UTC (there is no separate
timezone) Spanner client libraries convert timestamps to UTC before sending them
to Spanner. Data is always returned as UTC. Spanner's timestamp type is
essentially the same as `TIMESTAMPTZ`, except that there is no analog of
PostgreSQL's timezone parameter.

In other words, mapping PostgreSQL `TIMESTAMPTZ` to `TIMESTAMP` is fairly
straightforward, but care should be taken with PostgreSQL `TIMESTAMP` data
because Spanner clients will not drop the timezone.

## CHAR(n) and VARCHAR(n)

The semantics of fixed-length character types differ between PostgreSQL and
Spanner. The `CHAR(n)` type in PostgreSQL is padded with spaces. If a string
value smaller than the limit is stored, spaces will be added to pad it out to
the specified length. If a string longer than the specified length is stored,
and the extra characters are all spaces, then it will be silently
truncated. Moreover, trailing spaces are ignored when comparing two values. In
constrast, Spanner does not give special treatment to spaces, and the specified
length simply represents the maximum length that can be stored. This is close to
the semantics of PostgreSQL's `VARCHAR(n)`. However there are some minor
differences. For example, even `VARCHAR(n)` has some special treatment of
spaces: strings longer than the specified length are silently truncated if the
extra characters are all spaces.

## Storage Use

The tool maps several PostgreSQL types to Spanner types that use more storage.
For example, `SMALLINT` is a two-byte integer, but it maps to Spanner's `INT64`,
an eight-byte integer. This additional storage could be significant for large
arrays.

## Arrays

Spanner does not support multi-dimensional arrays. So while `TEXT[4]` maps to
`ARRAY<STRING(MAX)>` and `REAL ARRAY` maps to `ARRAY<FLOAT64>`, `TEXT[][]` maps
to `STRING(MAX)`.

Also note that PosgreSQL supports array limits, but the PostgreSQL
implementation ignores them. Spanner does not support array size limits, but
since they have no effect anyway, the tool just drops them.

## Primary Keys

Spanner requires primary keys for all tables. PostgreSQL recommends the use of
primary keys for all tables, but does not enforce this. When converting a table
without a primary key, Spanner migration tool will create a new primary key of type
INT64. By default, the name of the new column is `synth_id`. If there is already
a column with that name, then a variation is used to avoid collisions.

## NOT NULL Constraints

The tool preserves `NOT NULL` constraints. Note that Spanner does not require
primary key columns to be `NOT NULL`. However, in PostgreSQL, a primary key is a
combination of `NOT NULL` and `UNIQUE`, and so primary key columns from
PostgreSQL will be mapped to Spanner columns that are both primary keys and `NOT NULL`.

## Foreign Keys

The tool maps PostgreSQL foreign key constraints into Spanner foreign key constraints, and
preserves constraint names where possible. Note that Spanner requires foreign key
constraint names to be globally unique (within a database), but in postgres they only
have to be unique for a table, so we add a uniqueness suffix to a name if needed.
Spanner doesn't support `ON DELETE` and `ON UPDATE` actions, so we drop these.

## Default Values

Spanner does not currently support default values. We drop these
PostgreSQL features during conversion.

## Secondary Indexes

The tool maps PostgresSQL secondary indexes to Spanner secondary indexes, preserving
constraint names where possible. The tool also maps PostgreSQL `UNIQUE` constraints to
Spanner `UNIQUE` secondary indexes. Check [here](https://cloud.google.com/spanner/docs/migrating-postgres-spanner#indexes)
for more details.

## Other PostgreSQL features

PostgreSQL has many other features we haven't discussed, including functions,
sequences, procedures, triggers, (non-primary) indexes and views. The tool does
not support these and the relevant statements are dropped during schema
conversion.

See
[Migrating from PostgreSQL to Cloud Spanner](https://cloud.google.com/spanner/docs/migrating-postgres-spanner)
for a general discussion of PostgreSQL to Spanner migration issues.
Spanner migration tool follows most of the recommendations in that guide. The main
difference is that we map a few more types to `STRING(MAX)`.
