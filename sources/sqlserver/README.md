# HarbourBridge: SQLServer-to-Spanner Evaluation and Migration

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing PostgreSQL or MySQL database. This README provides
details of the tool's PostgreSQL capabilities. For general HarbourBridge information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-spanner-evaluation-and-migration).

## Example SQLServer Usage

HarbourBridge can either be used with pg_dump or it can be run directly
on a PostgreSQL database (via go's database/sql package).

The following examples assume a `harbourbridge` alias has been setup as described
in the [Installing HarbourBridge](https://github.com/cloudspannerecosystem/harbourbridge#installing-harbourbridge) section of the main README.

### Directly connecting to a PostgreSQL database

In this case, HarbourBridge connects directly to the Sql server database to
retrieve table schema and data. Set the `-source=sqlserver` and corresponding
source profile connection parameters `host`, `port`, `user`, `db_name` and
`password`.

For example to perform schema conversion, run

```sh
harbourbridge schema -source=postgres -source-profile="host=<>,port=<>,user=<>,db_name=<>"
```

Parameters `port` and `password` are optional. Port (`port`) defaults to `1433`
for PostgreSQL source. Password can be provided at the password prompt.
