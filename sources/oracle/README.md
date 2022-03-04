# HarbourBridge: Oracle-to-Spanner Evaluation and Migration

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing Oracle database. This README provides
details of the tool's Oracle capabilities. For general HarbourBridge information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-spanner-evaluation-and-migration).

Note that _'oracle'_ can be used as an identifier with the flag `-source` in the command line.

## Example Oracle Usage

HarbourBridge can be run directly on a Oracle database (via go's database/sql package).

The following examples assume a `harbourbridge` alias has been setup as described
in the [Installing HarbourBridge](https://github.com/cloudspannerecosystem/harbourbridge#installing-harbourbridge) section of the main README.

### Directly connecting to an Oracle database

In this case, HarbourBridge connects directly to the Oracle database to
retrieve table schema and data. Set the `-source=oracle` and corresponding
source profile connection parameters `host`, `port`, `user`, `db_name` and
`password`.

For example to perform schema conversion, run

```sh
harbourbridge schema -source=oracle -source-profile="host=<>,port=<>,user=<>,db_name=<>,password=<>"
```

In Oracle, USER is the account name, SCHEMA is the set of objects owned by that user. Oracle creates the SCHEMA object as part of the CREATE USER statement and the SCHEMA has the same name as the USER. 

db_name will be name of the Oracle service being used.


## Schema Conversion

| SQL_Server_Type        | Spanner_Type |
| ---------------------- | ------------ |
| NUMBER (* , 0)         | INT64        |
| FLOAT                  | FLOAT64      |
| BINARY_FLOAT           | FLOAT64      |
| BINARY_DOUBLE          | FLOAT64      |
| NUMBER (* , >0)        | NUMERIC      |
| CHAR                   | STRING       |
| NCHAR                  | STRING       |
| VARCHAR                | STRING       |
| VARCHAR2               | STRING       |
| NVARCHAR2              | STRING       |
| CLOB                   | STRING       |
| NCLOB                  | STRING       |
| LONG                   | STRING       |
| ROWID                  | STRING       |
| UROWID                 | STRING       |
| DATE                   | DATE         |
| TIMESTAMP              | TIMESTAMP    |
| BLOB                   | BYTES        |
| BFILE                  | BYTES        |
| RAW                    | BYTES        |
| LONG RAW               | BYTES        |
| XMLTYPE                | STRING       |
| INTERVAL YEAR          | STRING       |
| INTERVAL DAY           | STRING       |
| GEOMETRY               | STRING       |
| JSON                   | JSON         |



