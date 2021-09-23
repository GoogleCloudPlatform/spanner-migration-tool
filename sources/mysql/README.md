# HarbourBridge: Turnkey MySQL-to-Spanner Evaluation

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing PostgreSQL or MySQL database. This README provides
details of the tool's MySQL capabilities. For general HarbourBridge information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-turnkey-spanner-evaluation).

## Example MySQL Usage

The following examples assume `harbourbridge` has been added to your PATH
environment variable.

HarbourBridge can either be used with mysqldump or it can be run directly
on a MySQL database (via go's database/sql package).

### Using HarbourBridge with mysqldump

To use HarbourBridge on a MySQL database called mydb using mysqldump, run:

```sh
mysqldump mydb | harbourbridge -driver=mysqldump
```

The tool can also be applied to an existing mysqldump file:

```sh
harbourbridge -driver=mysqldump < my_mysqldump_file
```

To specify a particular Spanner instance to use, run:

```sh
mysqldump mydb | harbourbridge -driver=mysqldump -instance my-spanner-instance
```

By default, HarbourBridge will generate a new Spanner database name to populate.
You can override this and specify the database name to use by:

```sh
mysqldump mydb | harbourbridge -driver=mysqldump -dbname my-spanner-database-name
```

HarbourBridge generates a report file, a schema file, and a bad-data file (if
there are bad-data rows). You can control where these files are written by
specifying a file prefix. For example,

```sh
mysqldump mydb | harbourbridge -prefix mydb. -driver=mysqldump
```

will write files `mydb.report.txt`, `mydb.schema.txt`, and
`mydb.dropped.txt`. The prefix can also be a directory. For example,

```sh
mysqldump mydb | harbourbridge -prefix ~/spanner-eval-mydb/ -driver=mysqldump
```

would write the files into the directory `~/spanner-eval-mydb/`. Note
that HarbourBridge will not create directories as it writes these files.

### Directly connecting to a MySQL database

To use the tool directly on a MySQL database called mydb, run

```sh
harbourbridge -driver=mysql
```

It is assumed that _MYSQLHOST_, _MYSQLPORT_, _MYSQLUSER_, _MYSQLDATABASE_ environment
variables are set. Password can be specified either in the _MYSQLPWD_ environment
variable or provided at the password prompt.

Note that all of the options described in the previous section on using mysqldump
can also be used with "-driver=mysql".

## Schema Conversion

The HarbourBridge tool maps MySQL types to Spanner types as follows:

| MySQL Type                                        | Spanner Type    | Notes                           |
| ------------------------------------------------- | --------------- | ------------------------------- |
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
| `JSON`                                            | `STRING(MAX)`   |                                 |
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

### `DECIMAL` and `NUMERIC`

[Spanner's NUMERIC
type](https://cloud.google.com/spanner/docs/data-types#decimal_type) can store
up to 29 digits before the decimal point and up to 9 after the decimal point.
MySQL's NUMERIC type can potentially support higher precision than this, so
please verify that Spanner's NUMERIC support meets your application needs.  Note
that in MySQL, NUMERIC is implemented as DECIMAL, so the remarks about DECIMAL
apply equally to NUMERIC.

### `TIMESTAMP` and `DATETIME`

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

### `CHAR(n)` and `VARCHAR(n)`

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

### `SET`

MySQL `SET` is a string object that can hold muliple values, each of which must be
chosen from a list of permitted values specified when the table is created. `SET`
is being mapped to Spanner type `ARRAY<STRING>`. Validation of `SET` element values
will be dropped in Spanner. Thus for production use, validation needs to be done
in the application.

### `Spatial datatype`

MySQL spatial datatypes are used to represent geographic feature.
It includes `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTIPOLYGON`
and `GEOMETRYCOLLECTION` datatypes. Spanner does not support spatial data types.
This datatype are currently mapped to standard `STRING` Spanner datatype.

### Storage Use

The tool maps several MySQL types to Spanner types that use more storage.
For example, `SMALLINT` is a two-byte integer, but it maps to Spanner's `INT64`,
an eight-byte integer.

### Primary Keys

Spanner requires primary keys for all tables. MySQL recommends the use of
primary keys for all tables, but does not enforce this. When converting a table
without a primary key, HarbourBridge will create a new primary key of type
INT64. By default, the name of the new column is `synth_id`. If there is already
a column with that name, then a variation is used to avoid collisions.

### NOT NULL Constraints

The tool preserves `NOT NULL` constraints. Note that Spanner does not require
primary key columns to be `NOT NULL`. However, in MySQL, a primary key is a
combination of `NOT NULL` and `UNIQUE`, and so primary key columns from
MySQL will be mapped to Spanner columns that are both primary keys and `NOT NULL`.

### Foreign Keys

The tool maps MySQL foreign key constraints into Spanner foreign key constraints, and
preserves constraint names where possible. Since Spanner doesn't support `ON DELETE`
and `ON UPDATE` actions, we drop them.

### Default Values

Spanner does not currently support default values. We drop these
MySQL features during conversion.

### Secondary Indexes

The tool maps MySQL secondary indexes to Spanner secondary indexes, and preserves
constraint names where possible. Note that Spanner requires index key constraint
names to be globally unique (within a database), but in MySQL they only have to be
unique for a table, so we add a uniqueness suffix to a name if needed. The tool also
maps `UNIQUE` constraint into `UNIQUE` secondary index. Note that due to limitations of our
mysqldump parser, we are not able to handle key column ordering (i.e. ASC/DESC) in
mysqldump files. All key columns in mysqldump files will be treated as ASC.

### Other MySQL features

MySQL has many other features we haven't discussed, including functions,
sequences, procedures, triggers, (non-primary) indexes and views. The tool does
not support these and the relevant statements are dropped during schema
conversion.

See
[Migrating from MySQL to Cloud Spanner](https://cloud.google.com/solutions/migrating-mysql-to-spanner)
for a general discussion of MySQL to Spanner migration issues.
HarbourBridge follows most of the recommendations in that guide. The main
difference is that we map a few more types to `STRING(MAX)`.

## Data Conversion

### Timestamps and Timezones

As noted earlier when discussing [schema conversion of
TIMESTAMP](#timestamp), there are some subtle differences in how timestamps are
handled in MySQL and Spanner.

During data conversion, MySQL `TIMESTAMP` values are converted to UTC and
stored in Spanner. The conversion proceeds as follows. If the value has a
timezone offset, that timezone is respected during the conversion to UTC. If the value
does not have a timezone offset, then we look for any `set timezone` statements in the
mysqldump output and use the timezone offset specified. Otherwise, we use '+00:00' timezone offset (UTC).

### Strings, character set support and UTF-8

Spanner requires that `STRING` values be UTF-8 encoded. All Spanner functions
and operators that act on `STRING` values operate on Unicode characters rather
than bytes. Since we map many MySQL types (including `TEXT` and `CHAR`
types) to Spanner's `STRING` type, HarbourBridge is effectively a UTF-8 based
tool.

Note that the tool itself does not do any encoding/decoding or UTF-8 checks: it
passes through data from mysqldump to Spanner. Internally, we use Go's string
type, which supports UTF-8.

### Spatial datatypes support

As noted earlier when discussing [schema conversion of
Spatial datatype](#spatial-datatype), Spanner does not support spatial datatypes and are
mapped to `STRING(MAX)` Spanner type. Data conversion for spatial datatypes is different
for both `mysql` and `mysqldump` driver.

- MySQL information schema approach (`-driver=mysql`) : Data from MySQL is fetched using
  'ST_AsText(g)' function which converts a value in internal geometry format to its WKT(Well-Known Text)
  representation and returns the string result. This value will be stored as `STRING` in Spanner.
- MySQL dump approach (`-driver=mysqldump`) : Mysqldump will have the internal geometry data in
  binary format. It cannot be converted to WKT format and there is no proper method for mysqldump
  generation of spatial datatypes also. Thus, this value will just be fetched as a `TEXT` type and
  converted to Spanner type `STRING`.

Note that mysql information schema approach would be the recommended approach for data conversion of
spatial datatypes. For production use, you must store this data using standard data types, and implement
any searching/filtering logic in the application layer.
