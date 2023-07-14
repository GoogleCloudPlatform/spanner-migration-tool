# Spanner migration tool: SQLServer-to-Spanner Evaluation and Migration

Spanner migration tool is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing SQL Server database. This README provides
details of the tool's SQL Server capabilities. For general Spanner migration tool information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-spanner-evaluation-and-migration).

We currently do not support dump file mode for SQL Server. The only way to use Spanner migration tool with SQL Server is connecting directly.

We currently do not support clustered columnstore indexes in spanner, therefore such indexes will be skipped during the migration.

Note that either _'sqlserver'_ or _'mssql'_ can be used as an identifier with the flag `-source` in the command line.

## Example SQL Server Usage

Spanner migration tool can be run directly on a SQL Server database (via go's database/sql package).

The following examples assume a `harbourbridge` alias has been setup as described
in the [Installing HarbourBridge](https://github.com/cloudspannerecosystem/harbourbridge#installing-harbourbridge) section of the main README.

### Directly connecting to a SQL Server database

In this case, Spanner migration tool connects directly to the SQL Server database to
retrieve table schema and data. Set the `-source=sqlserver` and corresponding
source profile connection parameters `host`, `port`, `user`, `dbName` and
`password`.

For example, to perform schema conversion, run

```sh
harbourbridge schema -source=sqlserver -source-profile="host=<>,port=<>,user=<>,dbName=<>"
```

Parameters `port` and `password` are optional. Port (`port`) defaults to `1433`
for SQL Server source. Password can be provided at the password prompt.

## Schema Conversion

| SQL_Server_Type        | Spanner_Type |
| ---------------------- | ------------ |
| INT                    | INT64        |
| TINYINT                | INT64        |
| SMALLINT               | INT64        |
| BIGINT                 | INT64        |
| TIMESTAMP              | INT64        |
| ROWVERSION             | INT64        |
| BIT                    | BOOL         |
| FLOAT                  | FLOAT64      |
| REAL                   | FLOAT64      |
| NUMERIC                | NUMERIC      |
| DECIMAL                | NUMERIC      |
| MONEY                  | NUMERIC      |
| SMALLMONEY             | NUMERIC      |
| CHAR                   | STRING(1)    |
| NCHAR                  | STRING(N)    |
| VARCHAR                | STRING(MAX)  |
| NVARCHAR               | STRING(MAX)  |
| TEXT                   | STRING(MAX)  |
| NTEXT                  | STRING(MAX)  |
| DATE                   | DATE         |
| DATETIME               | TIMESTAMP    |
| DATETIME2              | TIMESTAMP    |
| SMALLDATETIME          | TIMESTAMP    |
| DATETIMEOFFSET         | TIMESTAMP    |
| TIME                   | STRING(MAX)  |
| BINARY                 | BYTES        |
| VARBINARY              | BYTES        |
| IMAGE                  | BYTES        |
| XML                    | STRING(MAX)  |
| UNIQUEIDENTIFIER       | STRING(MAX)  |
| SQL_VARIANT            | STRING(MAX)  |
| HIERARCHYID            | STRING(MAX)  |
| Spatial Geography Type | STRING(MAX)  |
| Spatial Geometry Types | STRING(MAX)  |

### `Spatial datatypes`

SQL Server supports `SPATIAL GEOGRAPHY` and `SPATIAL GEOMETRY` datatypes however, Spanner 
does not support spatial data types.
These datatype are currently mapped to standard `STRING` Spanner datatype.

### `TIMESTAMP`
The `TIMESTAMP` datatype (deprecated in the newer versions of SQL Server) 
was used for Row versioning. Hence, it is mapped to INT64 to keep it consistent
with the `ROWVERSION` data type.

### Storage Use

The tool maps several SQL Server types to Spanner types that use more storage.
For example, `SMALLINT` is a two-byte integer, but it maps to Spanner's `INT64`,
an eight-byte integer.

### Primary Keys

Spanner requires primary keys for all tables. SQL Server recommends the use of
primary keys for all tables, but does not enforce this. When converting a table
without a primary key:
- Spanner migration tool will check for `UNIQUE` constraints on the table. If found, it
will automatically pick any one of the unique constraints and convert it to a 
primary key.
- If no `UNIQUE` constraints are present, Spanner migration tool will create a new primary 
key column of type INT64. By default, the name of the new column is `synth_id`. 
- If there is already a column with that name, then a variation is used to avoid collisions.

### NOT NULL Constraints

The tool preserves `NOT NULL` constraints. Note that Spanner does not require
primary key columns to be `NOT NULL`. However, in SQL Server, a primary key is a
combination of `NOT NULL` and `UNIQUE`, and so primary key columns from
SQL Server will be mapped to Spanner columns that are both primary keys and `NOT NULL`.

### Foreign Keys

The tool maps SQL Server foreign key constraints into Spanner foreign key constraints, and
preserves constraint names where possible. Since Spanner doesn't support `DELETE CASCADE`
and `UPDATE CASCADE` actions, we drop them.

### Default Values

Spanner does not currently support default values. We drop these
SQL Server features during conversion.

### Secondary Indexes

The tool maps SQL Server non-clustered indexes to Spanner secondary indexes, and preserves
constraint names where possible. Note that Spanner requires index key constraint
names to be globally unique (within a database), but in SQL Server they only have to be
unique for a table, so we add a uniqueness suffix to a name if needed. The tool also
maps `UNIQUE` constraint into `UNIQUE` secondary index.

### Other SQL Server features

SQL Server has many other features we haven't discussed, including functions,
sequences, procedures, triggers and views which are currently not supported in Spanner. 
The tool does not support these and the relevant schema info is ignored during schema
conversion. 

## Data Conversion

### Strings, character set support and UTF-8

Spanner requires that `STRING` values be UTF-8 encoded. All Spanner functions
and operators that act on `STRING` values operate on Unicode characters rather
than bytes. Since we map many SQL Server types (including `VARCHAR`)
to Spanner's `STRING` type, Spanner migration tool is effectively a UTF-8 based
tool.
Internally, we use Go's string type, which supports UTF-8.

### Timestamps and Timezones

There are some subtle differences in how timestamps are
handled in SQL Server and Spanner.

During data conversion, SQL Server stores `SMALLDATETIME`, `DATETIME2`, `DATETIME` values 
in ISO 8601 format without offsets which is consistent with the way Spanner stores it. 
Hence, they are directly stored in Spanner. For `DATETIMEOFFSET`, the value contains an offset.
During conversion, the timezone offset is taken into account and the 
corresponding UTC time is saved.