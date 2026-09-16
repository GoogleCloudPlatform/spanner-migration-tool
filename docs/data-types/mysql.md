---
layout: default
title: MySQL
parent: Data Type Conversion
nav_order: 2
---

# Schema migration for MySQL
{: .no_toc }

Spanner migration tool (SMT) supports schema and data migrations from MySQL to Cloud Spanner for both **GoogleSQL** (default) and **PostgreSQL** dialects.

This document details the default type mappings, user-selectable target type overrides, dialect-specific nuances, and schema conversion rules applied by SMT.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Data Type Mapping

The table below summarizes the default mappings from MySQL data types to Cloud Spanner dialects, along with alternative types selectable in the web UI schema editor or via session file overrides.

| MySQL Type | GoogleSQL Default | PostgreSQL Default | Selectable Overrides | Notes & Conversion Issues |
|:---|:---|:---|:---|:---|
| `BOOL`, `BOOLEAN` | `BOOL` | `boolean` | `INT64` / `bigint`, `STRING(MAX)` / `varchar` | `BOOLEAN` is a MySQL alias for `TINYINT(1)`. |
| `TINYINT(1)` | `BOOL` | `boolean` | `INT64` / `bigint`, `STRING(MAX)` / `varchar` | Treated as boolean by default in MySQL conventions. |
| `TINYINT` ($N \ne 1$) | `INT64` | `bigint` | `STRING(MAX)` / `varchar`, `BOOL` (if $N=1$) | Storage size widened to 64 bits (`Widened`). |
| `SMALLINT`, `MEDIUMINT` | `INT64` | `bigint` | `STRING(MAX)` / `varchar` | Storage size widened to 64 bits (`Widened`). |
| `INT`, `INTEGER` | `INT64` | `bigint` | `STRING(MAX)` / `varchar` | Storage size widened to 64 bits (`Widened`). |
| `BIGINT` | `INT64` | `bigint` | `NUMERIC` / `numeric`, `STRING(MAX)` / `varchar` | Signed 64-bit integer. |
| `BIGINT UNSIGNED` | `INT64` | `bigint` | `NUMERIC` / `numeric`, `STRING(MAX)` / `varchar` | Values $> 2^{63}-1$ may overflow (`PossibleOverflow`). |
| `FLOAT` | `FLOAT32` | `real` | `FLOAT64` / `double precision`, `STRING(MAX)` / `varchar` | Can be widened to `FLOAT64` / `double precision`. |
| `DOUBLE`, `DOUBLE PRECISION` | `FLOAT64` | `double precision` | `STRING(MAX)` / `varchar` | |
| `DECIMAL`, `NUMERIC` | `NUMERIC` | `numeric` | `STRING(MAX)` / `varchar` | Spanner `NUMERIC` supports 29 integer and 9 scale digits. In PostgreSQL dialect, `NUMERIC` Primary Keys are not supported and widen to `varchar` (`NumericPKNotSupported`). |
| `CHAR` | `STRING(1)` | `character varying (1)` | `BYTES(1)` / `bytea` | Defaults to length 1 in MySQL. |
| `CHAR(N)` | `STRING(N)` | `character varying (N)` | `BYTES(N)` / `bytea` | Length $N$ is preserved. Space-padding semantics differ. |
| `VARCHAR(N)` | `STRING(N)` | `character varying (N)` | `BYTES(N)` / `bytea` | Length $N$ is preserved. |
| `VARCHAR` | `STRING(MAX)` | `character varying (2621440)` | `BYTES(MAX)` / `bytea` | Mapped to maximum allowed string length. |
| `TINYTEXT`, `TEXT` | `STRING(MAX)` | `character varying (2621440)` | `BYTES(MAX)` / `bytea` | |
| `MEDIUMTEXT`, `LONGTEXT` | `STRING(MAX)` | `character varying (2621440)` | `BYTES(MAX)` / `bytea` | |
| `BINARY` | `BYTES(1)` | `bytea` | `STRING(MAX)` / `varchar` | Bare `BINARY` in MySQL defaults to `BINARY(1)`. PG `bytea` has no length parameter. |
| `BINARY(N)` | `BYTES(N)` | `bytea` | `STRING(MAX)` / `varchar` | Length $N$ is preserved in GoogleSQL. |
| `VARBINARY(N)` | `BYTES(N)` | `bytea` | `STRING(MAX)` / `varchar` | Length $N$ is preserved in GoogleSQL. |
| `TINYBLOB` | `BYTES(255)` | `bytea` | `STRING(MAX)` / `varchar` | Fixed limit of 255 bytes. |
| `BLOB` | `BYTES(65535)` | `bytea` | `STRING(MAX)` / `varchar` | Fixed limit of 65,535 bytes (64 KiB). |
| `BLOB(N)` | `BYTES(N)` | `bytea` | `STRING(MAX)` / `varchar` | Preserved when migrating from a dump. Live MySQL servers report 65535. |
| `MEDIUMBLOB` | `BYTES(10485760)` | `bytea` | `STRING(MAX)` / `varchar` | Capped at Spanner's 10 MiB cell limit (`PossibleOverflow`). |
| `LONGBLOB` | `BYTES(10485760)` | `bytea` | `STRING(MAX)` / `varchar` | Capped at Spanner's 10 MiB cell limit (`PossibleOverflow`). |
| `BIT(1)` | `BOOL` | `boolean` | `INT64` / `bigint`, `STRING(MAX)` / `varchar` | Single-bit fields map directly to boolean. |
| `BIT(N)` ($N > 1$) | `BYTES(MAX)` | `bytea` | `INT64` / `bigint`, `STRING(MAX)` / `varchar` | Maps to `INT64` / `bigint` if selected; flags `PossibleOverflow` if $N = 64$. |
| `DATE` | `DATE` | `date` | `STRING(MAX)` / `varchar` | |
| `DATETIME` | `TIMESTAMP` | `timestamp with time zone` | `STRING(MAX)` / `varchar` | Flags `Datetime` issue due to MySQL absence of timezone storage. |
| `TIMESTAMP` | `TIMESTAMP` | `timestamp with time zone` | `STRING(MAX)` / `varchar` | UTC conversion applied during migration. |
| `TIME` | `STRING(MAX)` | `character varying (2621440)` | None | Spanner lacks a time-only type; flags `Time` issue. |
| `YEAR` | `STRING(MAX)` | `character varying (2621440)` | None | Spanner lacks a year-only type; flags `Time` issue. |
| `JSON` | `JSON` | `jsonb` | `STRING(MAX)` / `varchar`, `BYTES(MAX)` / `bytea` | |
| `ENUM` | `STRING(MAX)` | `character varying (2621440)` | None | Allowed values are not enforced by Spanner DDL. |
| `SET` | `ARRAY<STRING>` | `character varying (2621440)` | None | In PostgreSQL dialect, arrays are unsupported (`ArrayTypeNotSupported`) and map to `varchar`. |
| `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` | `STRING(MAX)` | `character varying (2621440)` | None | Spatial columns are exported as Well-Known Text (WKT) using `ST_AsText()` (`NoGoodType`). |

All other unrecognized data types map to `STRING(MAX)` (GoogleSQL) or `character varying (2621440)` (PostgreSQL) with a `NoGoodType` schema issue.

## BINARY, VARBINARY, and BLOBs

1. **Length Preservation**:
   - In MySQL, `BINARY` without an explicit length defaults to `BINARY(1)`. SMT maps it to `BYTES(1)` in GoogleSQL.
   - `BINARY(N)` and `VARBINARY(N)` retain their declared length $N$, mapping to `BYTES(N)` in GoogleSQL.
   - `BLOB(N)` declared with an explicit length in mysqldump (e.g., `BLOB(500)`) maps to `BYTES(500)` in GoogleSQL. Note that live MySQL servers discard length modifiers for blobs in `information_schema`, so live migrations fall back to the standard `BLOB` capacity (`BYTES(65535)`).

2. **Spanner Cell Size Limit (10 MiB)**:
   - MySQL `MEDIUMBLOB` supports up to 16 MiB ($2^{24}-1$ bytes) and `LONGBLOB` supports up to 4 GiB ($2^{32}-1$ bytes).
   - Cloud Spanner enforces a hard maximum of **10 MiB (10,485,760 bytes)** per column value.
   - SMT caps `MEDIUMBLOB` and `LONGBLOB` at `BYTES(10485760)` and raises a `PossibleOverflow` schema warning. If your MySQL database stores values exceeding 10 MiB, those writes will fail in Spanner and should instead be stored externally (e.g., in Google Cloud Storage).

3. **PostgreSQL Dialect (`BYTEA`)**:
   - In Cloud Spanner's PostgreSQL dialect (as well as native PostgreSQL), `BYTEA` is a variable-length binary type that does not support length modifiers (e.g. `BYTEA(N)` is invalid syntax).
   - All MySQL binary and blob types map to unparameterized `BYTEA` in the PostgreSQL dialect.

## BIT and Boolean Types

1. **Single-bit Columns**:
   - `BIT(1)`, `BOOL`, and `BOOLEAN` (which in MySQL is an alias for `TINYINT(1)`) map to Spanner `BOOL` (GoogleSQL) or `boolean` (PostgreSQL).
2. **Multi-bit Columns (`BIT(N)`)**:
   - By default, `BIT(N)` for $N > 1$ maps to `BYTES(MAX)` (GoogleSQL) or `BYTEA` (PostgreSQL).
3. **Optional Mapping to `INT64` / `bigint`**:
   - In the web UI schema editor or via session file overrides, users can choose to map `BIT(N)` to `INT64` (GoogleSQL) or `bigint` (PostgreSQL).
   - Because MySQL `BIT(64)` is unsigned (values up to $2^{64}-1$) while Spanner's `INT64` is signed (maximum value $2^{63}-1$), mapping a `BIT(64)` column to `INT64` generates a `PossibleOverflow` schema warning.

## Integers and Unsigned Types

1. **Integer Widening**:
   - MySQL integer types smaller than 64-bit (`TINYINT` for $N \ne 1$, `SMALLINT`, `MEDIUMINT`, `INT`, `INTEGER`) are widened to Spanner's 64-bit `INT64` (GoogleSQL) or `bigint` (PostgreSQL), generating a `Widened` warning.
2. **`BIGINT UNSIGNED`**:
   - MySQL `BIGINT UNSIGNED` supports values from `0` to `18,446,744,073,709,551,615` ($2^{64}-1$).
   - Spanner `INT64` supports signed values up to `9,223,372,036,854,775,807` ($2^{63}-1$).
   - SMT maps `BIGINT UNSIGNED` to `INT64` by default and logs a `PossibleOverflow` issue. To avoid data loss or write errors during data migration for values $> 2^{63}-1$, users can override the column mapping to `NUMERIC` or `STRING(MAX)` in the schema editor or session file.

## DECIMAL and NUMERIC

- [Spanner's NUMERIC type](https://cloud.google.com/spanner/docs/data-types#decimal_type) supports exact fixed-point numbers with up to 29 digits of integer precision and up to 9 digits of fractional scale (`NUMERIC(38, 9)`).
- MySQL `DECIMAL(M, D)` supports precision up to $M = 65$ digits and scale up to $D = 30$ digits. If your MySQL schema uses higher precision than Spanner `NUMERIC`, consider mapping to `STRING(MAX)`.
- **PostgreSQL Dialect Primary Key Restriction**: In Cloud Spanner's PostgreSQL dialect, `NUMERIC` columns cannot be used in primary keys. When a primary key column is `DECIMAL` or `NUMERIC`, SMT automatically widens it to `character varying (2621440)` and logs a `NumericPKNotSupported` issue.

## TIMESTAMP and DATETIME

- **MySQL `DATETIME`**: Stores calendar date and wall-clock time without timezone information. SMT maps `DATETIME` to Spanner `TIMESTAMP` (or `timestamp with time zone`) and logs a `Datetime` warning, because Spanner always converts and stores timestamps in UTC.
- **MySQL `TIMESTAMP`**: Converts values from the connection's time zone to UTC for storage and back to local time on retrieval. SMT tracks `SET TIME_ZONE` statements in mysqldump files and converts values to UTC timestamps in Spanner.
- In both cases, Spanner client libraries convert timestamps to UTC before sending them to Spanner, and values are returned in UTC.

## TIME and YEAR

- Cloud Spanner does not support a standalone `TIME` (time-of-day without date) or `YEAR` data type.
- SMT converts both types to `STRING(MAX)` (GoogleSQL) or `character varying (2621440)` (PostgreSQL) and logs a `Time` schema issue.

## CHAR(N) and VARCHAR(N)

- In MySQL, `CHAR(N)` right-pads stored strings with spaces and strips trailing spaces on retrieval.
- Cloud Spanner `STRING(N)` treats trailing spaces as significant characters and does not perform padding or stripping. The length $N$ in Spanner represents the maximum number of characters (not bytes).
- Both `CHAR(N)` and `VARCHAR(N)` map to `STRING(N)` (GoogleSQL) or `character varying (N)` (PostgreSQL), preserving the declared length $N$.

## ENUM and SET

- **`ENUM`**: Mapped to `STRING(MAX)` (GoogleSQL) or `character varying (2621440)` (PostgreSQL). The list of allowed enumeration values is dropped from the Spanner schema; validation should be handled in your application.
- **`SET`**:
  - In **GoogleSQL dialect**, `SET` is mapped to `ARRAY<STRING>`.
  - In **PostgreSQL dialect**, Spanner does not support array columns. SMT maps `SET` to `character varying (2621440)` and logs an `ArrayTypeNotSupported` schema issue.

## Spatial Data Types

- MySQL spatial data types (`GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION`) are mapped to `STRING(MAX)` (GoogleSQL) or `character varying (2621440)` (PostgreSQL) with a `NoGoodType` issue.
- During data migration, SMT queries spatial columns using `ST_AsText(column)` to export them in Well-Known Text (WKT) format for storage in Spanner.

## Primary Keys

Spanner requires primary keys for all tables. MySQL recommends the use of primary keys for all tables, but does not enforce this. When converting a table without a primary key, Spanner migration tool creates a synthetic primary key column:
- **GoogleSQL**: `synth_id INT64`
- **PostgreSQL**: `synth_id bigint`

By default, the name of the new column is `synth_id`. If there is already a column with that name, a variation (e.g. `synth_id_0`) is used to avoid collisions.

## NOT NULL Constraints

The tool preserves `NOT NULL` constraints from the source table. In MySQL, a primary key is implicitly `NOT NULL` and `UNIQUE`, so all primary key columns mapped from MySQL are configured as `NOT NULL` in Spanner.

## Foreign Keys

The tool maps MySQL foreign key constraints into Spanner foreign key constraints and ensures constraint names are globally unique within the database:
- **`ON DELETE`**: Actions (`CASCADE`, `SET NULL`, `RESTRICT`, `NO ACTION`) are preserved where supported by Spanner.
- **`ON UPDATE`**: Spanner does not support `ON UPDATE` actions, so these are dropped during schema migration.

## Default Values

The Spanner Migration Tool migrates `DEFAULT` values from MySQL columns whenever they can be mapped to valid Spanner expressions or literal constants. Any `DEFAULT` constraints that cannot be mapped to Spanner are dropped, and a `DefaultValue` warning is logged in the schema report. Users can review and edit default values in the Spanner draft editor before executing the migration.

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

The SKIP RANGE and START COUNTER WITH values can be set via both the web UI (recommended) and the CLI.

The Column tab of the web UI exposes fields to set the SKIP RANGE and START COUNTER WITH values. For more details, see [here](../ui/schema-conv/spanner-draft.md).

To set the SKIP RANGE and/or START COUNTER WITH values via the CLI, there are two options: either specify default
values to be used by all IDENTITY columns, or specify values on a per-column basis. Both options can be used in
conjuction with one another.

To specify default SKIP RANGE and/or START COUNTER WITH values to be used by all columns, include the following flags
in the `targetProfile` parameter: `defaultIdentitySkipRange` and `defaultIdentityStartCounterWith`, for example:
```sh
--targetProfile="instance=my-instance,defaultIdentitySkipRange=1000-5000,defaultIdentityStartCounterWith=100"
```
For more details on both flags, see [here](../cli/flags.md#target-profile).

To specify SKIP RANGE and/or START COUNTER WITH values on per-column basis via the CLI, do the following:
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

## Generated Columns

MySQL `STORED` and `VIRTUAL` generated columns are converted to Spanner generated columns:
- **GoogleSQL**: `AS (expression) STORED` (or virtual)
- **PostgreSQL**: `GENERATED ALWAYS AS (expression) STORED`

SMT sanitizes expression syntax to conform to Spanner's expression requirements. If an expression cannot be converted, the column is created without generation or flagged with an issue.

## Unsupported MySQL Features

The following MySQL database objects and features have no direct Cloud Spanner equivalent and are dropped or skipped during schema conversion:
- Stored procedures and functions
- Triggers
- Views
- Table partitioning definitions
- Non-standard character sets and collations (Spanner natively uses UTF-8)

For a general discussion of architectural differences and best practices, see [Migrating from MySQL to Cloud Spanner](https://cloud.google.com/solutions/migrating-mysql-to-spanner).
