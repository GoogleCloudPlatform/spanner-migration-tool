---
layout: default
title: PostgreSQL
parent: POC migrations
nav_order: 3
---

# POC migrations for PostgreSQL
{: .no_toc }

Spanner migration tool can either be used with pg_dump or it can be run directly
on a PostgreSQL database (via go's database/sql package).

{: .highlight }
Following instructions assume you have setup SMT by following the instructions in the [installation](../install.md) guide.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Using Spanner migration tool with pg_dump

The tool can used to migrate schema from an existing pg_dump file:

```sh
spanner-migration-tool schema -source=postgresql < my_pg_dump_file
```

You can use any of `postgresql`, `postgres`, or `pg` as the argument to the
`-source` flag. They all specify PostgreSQL as the source database.

This will generate a session file with `session.json` suffix. This file contains
schema mapping from source to destination. You will need to specify this file
during data migration. You also need to specify a particular Spanner instance and database to use
during data migration.

For example, run

```sh
spanner-migration-tool data -session=mydb.session.json -source=pg -target-profile="instance=my-spanner-instance,dbName=my-spanner-database-name" < my_pg_dump_file
```

You can also run Spanner migration tool in a schema-and-data mode, where it will perform both
schema and data migration. This is useful for quick evaluation when source
database size is small.

```sh
spanner-migration-tool schema-and-data -source=pg -target-profile="instance=my-spanner-instance" < my_pg_dump_file
```

Spanner migration tool generates a report file, a schema file, and a bad-data file (if
there are bad-data rows). You can control where these files are written by
specifying a file prefix. For example,

```sh
spanner-migration-tool schema -prefix=mydb. -source=postgres < my_pg_dump_file
```

will write files `mydb.report.txt`, `mydb.schema.txt`, and
`mydb.dropped.txt`. The prefix can also be a directory. For example,

```sh
spanner-migration-tool schema -prefix=~/spanner-eval-mydb/ -source=postgres < my_pg_dump_file
```

would write the files into the directory `~/spanner-eval-mydb/`. Note
that Spanner migration tool will not create directories as it writes these files.

### Sample dump files

If you don't have ready access to a PostgreSQL database, some example
dump files can be found [here](examples). The file
[cart.pg_dump](examples/cart.pg_dump) contains pg_dump for a very basic shopping cart application (just two tables, one for products and one for user carts). The file [singers.pg_dump](examples/singers.pg_dump) contains pg_dump output for a version of the [Cloud Spanner
singers](https://cloud.google.com/spanner/docs/schema-and-data-model#creating_a_table)
example.

## Directly connecting to a PostgreSQL database

In this case, Spanner migration tool connects directly to the PostgreSQL database to
retrieve table schema and data. Set the `-source=postgres` and corresponding
source profile connection parameters `host`, `port`, `user`, `dbName` and
`password`.

For example, to perform schema conversion, run

```sh
spanner-migration-tool schema -source=postgres -source-profile="host=<>,port=<>,user=<>,dbName=<>"
```

Parameters `port` and `password` are optional. Port (`port`) defaults to `5432`
for PostgreSQL source. Password can be provided at the password prompt.

Alternatively, you can also set environment variables `PGHOST`, `PGPORT`,
`PGUSER`, `PGDATABASE` for direct access. Password can be specified either in
the `PGPASSWORD` environment variable or provided at the password prompt.

Note that the various target-profile params described in the previous section
are also applicable in direct connect mode.