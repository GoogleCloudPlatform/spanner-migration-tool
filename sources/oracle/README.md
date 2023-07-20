# HarbourBridge: Oracle-to-Spanner Evaluation and Migration

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing Oracle database. This README provides
details of the tool's Oracle capabilities. For general Spanner migration tool information
see this [README](https://github.com/GoogleCloudPlatform/spanner-migration-tool#spanner-migration-tool-spanner-evaluation-and-migration).

We currently do not support dump file mode for Oracle. The only way to use HarbourBridge with OracleDB is connecting directly.

Note that _'oracle'_ can be used as an identifier with the flag `-source` in the command line.

## Example Oracle DB Usage

HarbourBridge can be run directly on a Oracle database (via go's database/sql package).

The following examples assume a `harbourbridge` alias has been setup as described
in the [Installing HarbourBridge](https://github.com/GoogleCloudPlatform/spanner-migration-tool#installing-spanner-migration-tool) section of the main README.

### Directly connecting to an Oracle database

In this case, HarbourBridge connects directly to the Oracle database to
retrieve table schema and data. Set the `-source=oracle` and corresponding
source profile connection parameters `host`, `port`, `user`, `dbName` and
`password`.

For example, to perform schema conversion, run

```sh
harbourbridge schema -source=oracle -source-profile="host=<>,port=<>,user=<>,dbName=<>,password=<>"
```

In Oracle DB, USER is the account name, SCHEMA is the set of objects owned by that user. Oracle creates the SCHEMA object as part of the CREATE USER statement and the SCHEMA has the same name as the USER. 

dbName will be the SID of the Database used. The Oracle System ID (SID) is used to uniquely identify a particular database on a system.

## Schema Conversion

| SQL_Server_Type        | Spanner_Type |
| ---------------------- | ------------ |
| NUMBER (* , 0)         | INT64        |
| FLOAT                  | FLOAT64      |
| BINARY_FLOAT           | FLOAT64      |
| BINARY_DOUBLE          | FLOAT64      |
| NUMBER (* , >0)        | NUMERIC      |
| CHAR                   | STRING(1)    |
| NCHAR                  | STRING(N)    |
| VARCHAR                | STRING(MAX)  |
| VARCHAR2               | STRING(MAX)  |
| NVARCHAR2              | STRING(MAX)  |
| CLOB                   | STRING(MAX)  |
| NCLOB                  | STRING(MAX)  |
| LONG                   | STRING(MAX)  |
| ROWID                  | STRING(MAX)  |
| UROWID                 | STRING(MAX)  |
| DATE                   | DATE         |
| TIMESTAMP              | TIMESTAMP    |
| BLOB                   | BYTES        |
| BFILE                  | BYTES        |
| RAW                    | BYTES        |
| LONG RAW               | BYTES        |
| XMLTYPE                | STRING(MAX)  |
| INTERVAL YEAR          | STRING(MAX)  |
| INTERVAL DAY           | STRING(MAX)  |
| GEOMETRY               | STRING(MAX)  |
| JSON                   | JSON         |



