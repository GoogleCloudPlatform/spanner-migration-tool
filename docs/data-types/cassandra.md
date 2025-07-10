---
layout: default
title: Cassandra
parent: Data Type Conversion
nav_order: 2
---

# Schema migration for Cassandra
{: .no_toc }

Spanner migration tool makes some assumptions while performing data type conversion from Cassandra to Spanner.
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

The Spanner migration tool maps Cassandra primitive types to Spanner types as follows:

| **Cassandra Type**                                    | **Spanner Type** | **Notes**                                                |
|:-------------------------------------------------:|:----------------:|:--------------------------------------------------------:|
| `ASCII`                                           | `STRING(MAX)`    |                                                          |
| `BIGINT`                                          | `INT64`          |                                                          |
| `BLOB`                                            | `BYTES(MAX)`     |                                                          |
| `BOOLEAN`                                         | `BOOL`           |                                                          |
| `COUNTER`                                         | `INT64`          | spanner does not support the counter behavior            |
| `DATE`                                            | `DATE`           |                                                          |
| `DECIMAL`, `VARINT`                               | `NUMERIC`        | potential changes of precision                           |
| `DOUBLE`                                          | `FLOAT64`        |                                                          |
| `FLOAT`                                           | `FLOAT32`        |                                                          |
| `INET`                                            | `STRING(MAX)`    |                                                          |
| `INT`, `SMALLINT`,<br/>`TINYINT`                  | `INT64`          | changes in storage size                                  |
| `TEXT`                                            | `STRING(MAX)`    |                                                          |
| `TIME`                                            | `INT64`          | spanner doesn't support a dedicated time data type       |
| `TIMESTAMP`                                       | `TIMESTAMP`      |                                                          |
| `UUID`, `TIMEUUID`                                | `STRING(MAX)`    | spanner doesn't validate the uuid                        |
| `VARCHAR`                                         | `STRING(MAX)`    |                                                          |

Unlike primitive types, Cassandra's collection types such as Maps, Sets, and Lists do not have direct, one-to-one equivalents in 
Spanner. Their mapping typically involves:

| **Cassandra Type**                                    | **Spanner Type** | **Notes**                                                |
|:-------------------------------------------------:|:----------------:|:--------------------------------------------------------:|
| `SET`                                             | `ARRAY`          | spanner doesn't support a dedicated set data type. Use ARRAY columns to represent a set.   |
| `LIST`                                            | `ARRAY`          | use ARRAY to store a list of typed objects.                                                |
| `MAP`                                             | `JSON`           | spanner doesn't support a dedicated map type. Use JSON columns to represent maps           |

Spanner does not support `duration` datatype of Cassandra. Along with `duration`
datatype, all other types map to `STRING(MAX)`.
 
## DECIMAL and VARINT

[Spanner's NUMERIC
type](https://cloud.google.com/spanner/docs/data-types#decimal_type) can store
up to 29 digits before the decimal point and up to 9 after the decimal point.
Cassandra's DECIMAL type can potentially support higher precision than this, so
please verify that Spanner's NUMERIC support meets your application needs.  Note
that the remarks about DECIMAL apply equally to VARINT.

## UUID and TIMEUUID

Cassandra has two primary identifier types often used for unique keys: `UUID` and `TIMEUUID`. 
UUID is a standard Type 4 UUID, generally randomly generated. TIMEUUID is a Type 1 UUID, which 
embeds a timestamp and is time-ordered, providing a natural chronological sorting. Cassandra's 
drivers and functions are aware of the internal structure of these types.

Spanner does not have a native `UUID` or `TIMEUUID` data type. Instead, these are typically 
stored using the `STRING` type (for the hexadecimal string representation) 
or `BYTES` (specifically `BYTES(16)` for the 16-byte UUID value)

When storing `UUID` or `TIMEUUID` data in Spanner, Spanner does not perform intrinsic validation 
of the UUID's internal structure or format (e.g., checking for correct version bits, variant bits, 
or a valid time component for TIMEUUID) from the source.

## COUNTER

Cassandra's `COUNTER` type provides atomic, distributed increments/decrements.

Spanner doesn't have a direct equivalent to Cassandra's `COUNTER`. While we typically map this 
data to an `INT64` column in Spanner, you'll need to implement counter logic within your 
application's transactions (read, increment, write) to ensure correctness.

## DURATION

Cassandra has a `DURATION` type for periods of time. Spanner doesn't have a native equivalent, 
so we typically map this to a `STRING` (e.g., ISO 8601 format). So please ensure that your 
application handles this.

## TIME

Cassandra has a `TIME` type for the time of day (nanoseconds since midnight). Spanner doesn't 
have a native equivalent, so we typically map this to an `INT64` to store nanosecond duration. 
So please ensure that your application handles this.

## SET and LIST

Cassandra uses `SET` (an unordered collection of unique elements) and 
`LIST` (an ordered collection of non-unique elements).

Both of these are typically mapped to Spanner's `ARRAY` type (e.g., `SET<TEXT>` to 
`ARRAY<STRING(MAX)>`, `LIST<INT>` to `ARRAY<INT64>`). When mapping `SET` to `ARRAY`, 
note that Spanner's ARRAY is ordered and allows duplicates. Therefore, your application 
must handle uniqueness if required.

## MAP

Cassandra uses `MAP` for storing typed key-value pairs. Spanner does not have a native `MAP` type. 
Cassandra's `MAP` typically maps to Spanner's `JSON` type. Unlike Cassandra, Spanner does not 
validate the internal `JSON` structure or types, so your application must ensure data integrity.

## Storage Use

The tool maps several Cassandra types to Spanner types that use more storage.
For example, `SMALLINT` is a two-byte integer, but it maps to Spanner's `INT64`,
an eight-byte integer.

## Primary Keys

Spanner requires primary keys for all tables. Spanner's primary key is derived
as a composite of the Cassandra partition key and clustering key.

## Column Nullability

Cassandra does not enforce all columns on all rows, so corresponding Spanner columns are 
created as `NULLABLE` by default. Spanner primary key columns, however, are inherently `NOT NULL`. 
We can explicitly define other columns as NOT NULL in Spanner if Cassandra data guarantees a value.

## Foreign Keys

Cassandra does not support native foreign key constraints. Therefore, no such constraints exist 
to convert when migrating from Cassandra to Spanner.

## Secondary Indexes

The tool currently doesn't support the migration of Cassandra secondary indexes to Spanner secondary indexes.

## Other Cassandra Types
Cassandra's other complex types, such as nested collection types and User Defined Types (UDTs), are currently
not natively supported in Spanner. By default, these types are mapped to `STRING(MAX)`.

## Note
See [Migrating from Cassandra to Cloud Spanner](https://cloud.google.com/spanner/docs/non-relational/migrate-from-cassandra-to-spanner)
for details on data migration since currently SMT supports schema only migration.
