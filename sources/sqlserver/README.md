# HarbourBridge: SQLServer-to-Spanner Evaluation and Migration

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing SQL Server database. This README provides
details of the tool's SQLserver capabilities. For general HarbourBridge information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-spanner-evaluation-and-migration).

Note that either _'sqlserver'_ or _'mssql'_ can be used as an identifier with the flag `-source` in the command line.

## Example SQLServer Usage

HarbourBridge can be run directly on a sqlserver database (via go's database/sql package).

The following examples assume a `harbourbridge` alias has been setup as described
in the [Installing HarbourBridge](https://github.com/cloudspannerecosystem/harbourbridge#installing-harbourbridge) section of the main README.

### Directly connecting to a Sql server database

In this case, HarbourBridge connects directly to the Sql server database to
retrieve table schema and data. Set the `-source=sqlserver` and corresponding
source profile connection parameters `host`, `port`, `user`, `db_name` and
`password`.

For example to perform schema conversion, run

```sh
harbourbridge schema -source=sqlserver -source-profile="host=<>,port=<>,user=<>,db_name=<>"
```

Parameters `port` and `password` are optional. Port (`port`) defaults to `1433`
for SQLserver source. Password can be provided at the password prompt.

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
| CHAR                   | STRING       |
| NCHAR                  | STRING       |
| VARCHAR                | STRING       |
| NVARCHAR               | STRING       |
| TEXT                   | STRING       |
| NTEXT                  | STRING       |
| DATE                   | DATE         |
| DATETIME               | TIMESTAMP    |
| DATETIME2              | TIMESTAMP    |
| SMALLDATETIME          | TIMESTAMP    |
| DATETIMEOFFSET         | TIMESTAMP    |
| TIME                   | STRING       |
| BINARY                 | BYTES        |
| VARBINARY              | BYTES        |
| IMAGE                  | BYTES        |
| XML                    | STRING       |
| UNIQUEIDENTIFIER       | STRING       |
| SQL_VARIANT            | STRING       |
| HIERARCHYID            | STRING       |
| Spatial Geography Type | STRING       |
| Spatial Geometry Types | STRING       |
