# Running Integration Tests Locally

The integration tests involve performing actual migrations from a variety of sources (including actual databases),
targeted towards a local emulated instance of spanner. The following describes the steps needed to set up all the
resources required for running the integration tests.

## Setup Source Databases

The following integration tests expect an actually running database to use as the source:
- `testing/dynamodb/streaming`
- `testing/mysql`
- `testing/oracle`
- `testing/postgres`
- `testing/sqlserver`

The schema and data for these source databases can be found in the `test_data` directory, as described below.

### DynamoDB

TODO

### MySQL

Start up a MySQL database server (either installed locally, via docker, an external server, etc.).

Initialize the database server with the following test data SQL scripts:
- `test_data/mysqldump.test.out`: A database must already exist (no specific DB name required); populates existing
  database
- `test_data/mysql_interleave_dump.test.out`: Creates and populates a database named `test_interleave_table_data`
- `test_data/mysql_foreignkeyaction_dump.test.out`: Creates and populates a database named `test_foreign_key_action_data`
- `test_data/mysql_checkconstraint_dump.test.out`: Creates and populates a database named `test_mysql_checkconstraint`

In the terminal from which you'll be running the tests, set the following environment variables:
```sh
    export MYSQLHOST=<MySQL DB server host>
    export MYSQLPORT=<MySQL DB server port> # not required if default MySQL port of 3306 is being used
    export MYSQLUSER=<MySQL DB server username>
    export MYSQLPWD=<MySQL DB server password>
    export MYSQLDATABASE=test_interleave_table_data
    export MYSQLDB_FKACTION=test_foreign_key_action_data
    export MYSQLDB_CHECK_CONSTRAINT=test_mysql_checkconstraint
```

### Oracle

Start up an Oracle Express Edition database server (either installed locally, via docker, an external server, etc.).

Initialize the database server with the following test data SQL scripts:
- `test_data/oracle.test.out`: Creates user "sti" with password "test1" and populates tables for that user

### Postgres

Start up a PostgreSQL database server (either installed locally, via docker, an external server, etc.).

Initialize the database server with the following test data SQL scripts:
- `test_data/pg_dump.test.out`: A database must already exist (initial postgres database is acceptable); populates
  existing database

In the terminal from which you'll be running the tests, set the following environment variables:
```sh
    export PGHOST=<Postgres DB server host>
    export PGPORT=<Postgres DB server port>
    export PGUSER=<Postgres DB server user>
    export PGPASSWORD=<Postgres DB server password>
    export PGDATABASE=<Postgres DB server database name>
```

### SQLServer

Start up a SQLServer database server (either installed locally, via docker, etc.). It must be available via hostname
`localhost` and user "sa" must exist (these are hard-coded in the test).

Initialize the database server with the following test data SQL scripts:
- `test_data/sqlserver.test.out`: Creates and populates a database named `SqlServer_IntTest`

In the terminal from which you'll be running the tests, set the following environment variables:
```sh
    export MSSQL_SA_PASSWORD=<Password for user "sa">
```


## Setup the Spanner emulator

Install and setup the spanner emulator as described
[here](https://docs.cloud.google.com/spanner/docs/emulator#emulator-for-gcloud).

Start the emulator:
```sh
    gcloud emulators spanner start
```

In another terminal, create a Spanner instance to use for testing (here we use the name test-instance, but this may be
changed to any other name):
```sh
    gcloud spanner instances create <instance name> --config=emulator-config --description=<brief instance description> --nodes=1
```

In the terminal from which you'll be running the tests, set the following environment variables (note, the instance name
is the same as the one created above):
```sh
    export SPANNER_EMULATOR_HOST=localhost:9010
    export SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID=<instance name>
    export SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID=<project id>
```

## Running the Tests

The integration tests are all found in the `testing` directory. Prior to running the integration tests, ensure you have
all required components (databases, emulated spanner, etc) up and running, and the relevant environment variables set as
described above.

Note that if you are only running certain unit tests, you only need to perform the setup for those specific tests (for
example, if you only want to run the the tests in `testing/mysql`, you only need to have a running MySQL instance with
the relevant environment variables set, there's no need to have any other database running).

Once you have everything setup as required, run the integration tests as follows:
```sh
    go test -v ./testing/...
```

Note that running all tests (using `go test -v ./...`) will also run the integration tests; if the
`SPANNER_EMULATOR_HOST` environment variable is **not** set, the integration tests will simply be skipped.
