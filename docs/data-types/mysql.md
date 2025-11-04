---
layout: default
title: MySQL
parent: Data Type Conversion
nav_order: 2
---

# Schema migration for MySQL
{: .no_toc }

Spanner migration tool makes some assumptions while performing data type conversion from MySQL to Spanner.
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

The Spanner migration tool maps MySQL types to Spanner types as follows:

|                  **MySQL Type**                   | **Spanner Type**  | **Notes**                                                |
|:-------------------------------------------------:|:-----------------:|:--------------------------------------------------------:|
|        `BOOL`, `BOOLEAN`,<br/>`TINYINT(1)`        |      `BOOL`       |                                                          |
|                     `BIGINT`                      |      `INT64`      |                                                          |
|               `BINARY`, `VARBINARY`               |   `BYTES(MAX)`    |                                                          |
|                      `BLOB`                       |  `BYTES(65535)`   |                                                          |
|                     `BLOB(N)`                     |    `BYTES(N)`     |                                                          |
|                   `MEDIUMBLOB`                    |     `BYTES(10485760)`     |                                                          |
|                  `MEDIUMBLOB(N)`                  |    `BYTES(N)`     |                                                          |
|                    `TINYBLOB`                     |   `BYTES(255)`    |                                                          |
|                   `TINYBLOB(N)`                   |    `BYTES(N)`     |                                                          |
|                    `LONGBLOB`                     | `BYTES(10485760)` |                                                          |
|                   `LONGBLOB(N)`                   |    `BYTES(N)`     |                                                          |
|                       `BIT`                       |   `BYTES(MAX)`    | BIT(1) converts to BOOL, other cases map to BYTES        |
|                      `CHAR`                       |    `STRING(1)`    | CHAR defaults to length 1                                |
|                     `CHAR(N)`                     |    `STRING(N)`    | differences in treatment of fixed-length character types |
|                      `DATE`                       |      `DATE`       |                                                          |
|                    `DATETIME`                     |    `TIMESTAMP`    | differences in treatment of timezones                    |
|               `DECIMAL`, `NUMERIC`                |     `NUMERIC`     | potential changes of precision                           |
|                     `DOUBLE`                      |     `FLOAT64`     |                                                          |
|                      `ENUM`                       |   `STRING(MAX)`   |                                                          |
|                      `FLOAT`                      |     `FLOAT32`     |                                                          |
| `INTEGER`, `MEDIUMINT`,<br/>`TINYINT`, `SMALLINT` |      `INT64`      | changes in storage size                                  |
|                      `JSON`                       |      `JSON`       |                                                          |
|                       `SET`                       |  `ARRAY<STRING>`  | SET only supports string values                          |
| `TEXT`, `MEDIUMTEXT`,<br/>`TINYTEXT`, `LONGTEXT`  |   `STRING(MAX)`   |                                                          |
|                    `TIMESTAMP`                    |    `TIMESTAMP`    |                                                          |
|                     `VARCHAR`                     |   `STRING(MAX)`   |                                                          |
|                   `VARCHAR(N)`                    |    `STRING(N)`    | differences in treatment of fixed-length character types |


Spanner does not support `spatial` datatypes of MySQL. Along with `spatial`
datatypes, all other types map to `STRING(MAX)`.

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
preserves constraint names where possible. Since Spanner doesn't support `ON UPDATE` action, we drop it.

## Default Values

The Spanner Migration Tool automatically migrates all `DEFAULT` values from a MySQL source
to a GoogleSQL destination, provided they can be mapped without modification.
Any `DEFAULT` constraints that cannot be mapped are dropped, and a warning is issued. 
Users can edit the column to change the `DEFAULT` constraints. The validity of the `DEFAULT`
constraints will be verified when users try to move to the Prepare Migration page. In case
of any errors users will not be able to proceed until all `DEFAULT` constraints are valid.

## Check Constraints

While Spanner supports check constraints, the Spanner migration tool currently migrates all valid check constraints from MySQL to Spanner.

During schema conversion, invalid check constraints are dropped, and warnings are logged in the Issues & Suggestions tab. If users add invalid constraints in the Spanner draft and proceed to the prepare migration phase, these constraints are retained but logged as errors for correction. The migration proceeds only after errors are resolved, ensuring a smooth and compatible process.  

> Note: As check constraints were introduced with MySQL version 8.0.16, the Spanner migration tool will automatically include these constraints in the Spanner draft for databases using this version or later. For MySQL versions prior to 8.0.16, where check constraints are not supported, users will need to manually incorporate any required check constraints into the Spanner draft. This approach ensures that all necessary constraints are accurately represented in the Spanner environment, tailored to the specific needs of the database.

## Secondary Indexes

The tool maps MySQL secondary indexes to Spanner secondary indexes, and preserves
constraint names where possible. Note that Spanner requires index key constraint
names to be globally unique (within a database), but in MySQL they only have to be
unique for a table, so we add a uniqueness suffix to a name if needed. The tool also
maps `UNIQUE` constraint into `UNIQUE` secondary index. Note that due to limitations of our
mysqldump parser, we are not able to handle key column ordering (i.e. ASC/DESC) in
mysqldump files. All key columns in mysqldump files will be treated as ASC.

## Auto-Increment Columns

The tool maps auto-increment columns to [Spanner IDENTITY
columns](https://cloud.google.com/spanner/docs/primary-key-default-value#identity-columns).
Users need to set the SKIP RANGE and/or START COUNTER WITH values to avoid duplicate key errors.

The SKIP RANGE and START COUNTER WITH values can be set most via both the web UI (recommended) and the CLI.

The Column tab of the web UI exposes fields to set the SKIP RANGE and START COUNTER WITH values. For more details, see [here](../ui/schema-conv/spanner-draft.md).

To set the SKIP RANGE and/or START COUNTER WITH values via the CLI, the recommended steps are as follows:
- Do a dry-run schema-only migration to generate a session JSON file:
```sh
    spanner-migration-tool schema -dry-run ...
```
- Open the resulting session file and find the relevant column definition(s) in the `ColDefs` collection of the table
  it belongs to
- Set the appropriate fields in that column's `AutoGen.AutoIncrementOptions` node. All three values are expected to be
  strings containing a numeric value. For example:
```json
    {
        "SpSchema": {
            "table1": {
                "Name": "SomeTable",
                "ColDefs": {
                    "column1": {
                        "Name": "some_column",
                        "AutoGen": {
                            "Name": "Auto Increment",
                            "GenerationType": "Auto Increment",
                            "AutoIncrementOptions": {
                                "SkipRangeMin": "1000",
                                "SkipRangeMax": "10000",
                                "StartCounterWith": "500"
                            }
                        },
                        ...
                    },
                    ...
                },
                ...
            },
            ...
        },
        ...
    }
```
- Save the session file and run your desired migration using the updated session file:
```sh
    spanner-migration-tool schema -session=<path to session file> ...
```

## Other MySQL features

MySQL has many other features we haven't discussed, including functions procedures, triggers, (non-primary) indexes and views. The tool does
not support these and the relevant statements are dropped during schema
conversion.

See [Migrating from MySQL to Cloud Spanner](https://cloud.google.com/solutions/migrating-mysql-to-spanner)
for a general discussion of MySQL to Spanner migration issues.
Spanner migration tool follows most of the recommendations in that guide. The main
difference is that we map a few more types to `STRING(MAX)`.
