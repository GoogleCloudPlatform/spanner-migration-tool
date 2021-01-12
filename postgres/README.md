# HarbourBridge: Turnkey PostgreSQL-to-Spanner Evaluation

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing PostgreSQL or MySQL database. This README provides
details of the tool's PostgreSQL capabilities. For general HarbourBridge information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-turnkey-spanner-evaluation).

## Example PostgreSQL Usage

The following examples assume `harbourbridge` has been added to your PATH
environment variable.

HarbourBridge can either be used with pg_dump or it can be run directly
on a PostgreSQL database (via go's database/sql package).

### Using HarbourBridge with pg_dump

To use HarbourBridge on a PostgreSQL database called mydb using pg_dump output,run:

```sh
pg_dump mydb | harbourbridge -driver=pg_dump
```

The tool can also be applied to an existing pg_dump file:

```sh
harbourbridge -driver=pg_dump < my_pg_dump_file
```

To specify a particular Spanner instance to use, run:

```sh
pg_dump mydb | harbourbridge -driver=pg_dump -instance my-spanner-instance
```

By default, HarbourBridge will generate a new Spanner database name to populate.
You can override this and specify the database name to use by:

```sh
pg_dump mydb | harbourbridge -driver=pg_dump -dbname my-spanner-database-name
```

HarbourBridge generates a report file, a schema file, and a bad-data file (if
there are bad-data rows). You can control where these files are written by
specifying a file prefix. For example,

```sh
pg_dump mydb | harbourbridge -driver=pg_dump -prefix mydb.
```

will write files `mydb.report.txt`, `mydb.schema.txt`, and
`mydb.dropped.txt`. The prefix can also be a directory. For example,

```sh
pg_dump mydb | harbourbridge -driver=pg_dump -prefix ~/spanner-eval-mydb/
```

would write the files into the directory `~/spanner-eval-mydb/`. Note
that HarbourBridge will not create directories as it writes these files.

### Directly connecting to a PostgreSQL database

To use the tool directly on a PostgresSQL database called mydb, run

```sh
harbourbridge -driver=postgres
```

It is assumed that _PGHOST_, _PGPORT_, _PGUSER_, _PGDATABASE_ environment
variables are set. Password can be specified either in the _PGPASSWORD_ environment
variable or provided at the password prompt.

Note that all of the options described in the previous section on using pg_dump can
also be used with "-driver=postgres".

## Schema Conversion

The HarbourBridge tool maps PostgreSQL types to Spanner types as follows:

| PostgreSQL Type    | Spanner Type           | Notes                                     |
| ------------------ | ---------------------- | ----------------------------------------- |
| `BOOL`             | `BOOL`                 |                                           |
| `BIGINT`           | `INT64`                |                                           |
| `BIGSERIAL`        | `INT64`                | a                                         |
| `BYTEA`            | `BYTES(MAX)`           |                                           |
| `CHAR`             | `STRING(1)`            | CHAR defaults to length 1                 |
| `CHAR(N)`          | `STRING(N)`            | c                                         |
| `DATE`             | `DATE`                 |                                           |
| `DOUBLE PRECISION` | `FLOAT64`              |                                           |
| `INTEGER`          | `INT64`                | s                                         |
| `NUMERIC`          | `NUMERIC`              | p                                         |
| `REAL`             | `FLOAT64`              | s                                         |
| `SERIAL`           | `INT64`                | a, s                                      |
| `SMALLINT`         | `INT64`                | s                                         |
| `TEXT`             | `STRING(MAX)`          |                                           |
| `TIMESTAMP`        | `TIMESTAMP`            | t                                         |
| `TIMESTAMPTZ`      | `TIMESTAMP`            |                                           |
| `VARCHAR`          | `STRING(MAX)`          |                                           |
| `VARCHAR(N)`       | `STRING(N)`            | c                                         |
| `ARRAY(`pgtype`)`  | `ARRAY(`spannertype`)` | if scalar type pgtype maps to spannertype |

All other types map to `STRING(MAX)`. Some of the mappings in this table
represent potential changes of precision (marked p), dropped autoincrement
functionality (marked a), differences in treatment of timezones (marked t),
differences in treatment of fixed-length character types (marked c), and changes
in storage size (marked s). We discuss these, as well as other limits and notes
on schema conversion, in the following sections.

### `NUMERIC`

[Spanner's NUMERIC
type](https://cloud.google.com/spanner/docs/data-types#decimal_type) can store
up to 29 digits before the decimal point and up to 9 after the decimal point.
PostgreSQL's NUMERIC type can potentially support higher precision that this, so
please verify that Spanner's NUMERIC support meets your application needs.

### `BIGSERIAL` and `SERIAL`

Spanner does not support autoincrementing types, so these both map to `INT64`
and the autoincrementing functionality is dropped.

### `TIMESTAMP`

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

### `CHAR(n)` and `VARCHAR(n)`

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

### Storage Use

The tool maps several PostgreSQL types to Spanner types that use more storage.
For example, `SMALLINT` is a two-byte integer, but it maps to Spanner's `INT64`,
an eight-byte integer. This additional storage could be significant for large
arrays.

### Arrays

Spanner does not support multi-dimensional arrays. So while `TEXT[4]` maps to
`ARRAY<STRING(MAX)>` and `REAL ARRAY` maps to `ARRAY<FLOAT64>`, `TEXT[][]` maps
to `STRING(MAX)`.

Also note that PosgreSQL supports array limits, but the PostgreSQL
implementation ignores them. Spanner does not support array size limits, but
since they have no effect anyway, the tool just drops them.

### Primary Keys

Spanner requires primary keys for all tables. PostgreSQL recommends the use of
primary keys for all tables, but does not enforce this. When converting a table
without a primary key, HarbourBridge will create a new primary key of type
INT64. By default, the name of the new column is `synth_id`. If there is already
a column with that name, then a variation is used to avoid collisions.

### NOT NULL Constraints

The tool preserves `NOT NULL` constraints. Note that Spanner does not require
primary key columns to be `NOT NULL`. However, in PostgreSQL, a primary key is a
combination of `NOT NULL` and `UNIQUE`, and so primary key columns from
PostgreSQL will be mapped to Spanner columns that are both primary keys and `NOT NULL`.

### Foreign Keys

The tool maps PostgreSQL foreign key constraints into Spanner foreign key constraints, and
preserves constraint names where possible. Note that Spanner requires foreign key
constraint names to be globally unique (within a database), but in postgres they only
have to be unique for a table, so we add a uniqueness suffix to a name if needed.
Spanner doesn't support `ON DELETE` and `ON UPDATE` actions, so we drop these.

### Default Values

Spanner does not currently support default values. We drop these
PostgreSQL features during conversion.

### Secondary Indexes

The tool maps PostgresSQL secondary indexes to Spanner secondary indexes, preserving
constraint names where possible. The tool also maps PostgreSQL UNIQUE constraints to
Spanner UNIQUE secondary indexes. Check [here](https://cloud.google.com/spanner/docs/migrating-postgres-spanner#indexes)
for more details.

### Other PostgreSQL features

PostgreSQL has many other features we haven't discussed, including functions,
sequences, procedures, triggers, (non-primary) indexes and views. The tool does
not support these and the relevant statements are dropped during schema
conversion.

See
[Migrating from PostgreSQL to Cloud Spanner](https://cloud.google.com/spanner/docs/migrating-postgres-spanner)
for a general discussion of PostgreSQL to Spanner migration issues.
HarbourBridge follows most of the recommendations in that guide. The main
difference is that we map a few more types to `STRING(MAX)`.

## Data Conversion

### Timestamps and Timezones

As noted earlier when discussing [schema conversion of
TIMESTAMP](#timestamp), there are some subtle differences in how timestamps are
handled in PostgreSQL and Spanner.

During data conversion, PostgreSQL `TIMESTAMPTZ` values are converted to UTC and
stored in Spanner. The conversion proceeds as follows. If the value has a
timezone, that timezone is respected during the conversion to UTC. If the value
does not have a timezone, then we look for any `set timezone` statements in the
pg_dump output and use the timezone specified. Otherwise, we use the `TZ`
environment variable as the timezone, and failing that, we use the local system
timezone default (as determined by Go).

In constrast, conversion of PostgreSQL `TIMESTAMP` values proceeds by ignoring
any timezone information and just treating the value as UTC and storing it in
Spanner.

### Strings, character set support and UTF-8

Spanner requires that `STRING` values be UTF-8 encoded. All Spanner functions
and operators that act on `STRING` values operate on Unicode characters rather
than bytes. Since we map many PostgreSQL types (including `TEXT` and `CHAR`
types) to Spanner's `STRING` type, HarbourBridge is effectively a UTF-8 based
tool.

Note that the tool itself does not do any encoding/decoding or UTF-8 checks: it
passes through data from pg_dump to Spanner. Internally, we use Go's string
type, which supports UTF-8.
