---
layout: default
title: MySQL
parent: POC migrations
nav_order: 2
---

# POC migrations for MySQL
{: .no_toc }

Spanner migration tool can either be used with mysqldump or it can be run directly
on a MySQL database (via go's database/sql package).

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

## Using Spanner migration tool with mysqldump

The tool can be used to migrate schema from an existing mysqldump file:

```sh
spanner-migration-tool schema -source=mysql < my_mysqldump_file
```

This will generate a session file with `session.json` suffix. This file contains
schema mapping from source to destination. You will need to specify this file
during data migration. You also need to specify a particular Spanner instance and database to use
during data migration.

For example, run

```sh
spanner-migration-tool data -session=mydb.session.json -source=mysql -target-profile="instance=my-spanner-instance,dbName=my-spanner-database-name" < my_mysqldump_file
```

You can also run Spanner migration tool in a schema-and-data mode, where it will perform both
schema and data migration. This is useful for quick evaluation when source
database size is small.

```sh
spanner-migration-tool schema-and-data -source=mysql -target-profile="instance=my-spanner-instance" < my_mysqldump_file
```

Spanner migration tool generates a report file, a schema file, and a bad-data file (if
there are bad-data rows). You can control where these files are written by
specifying a file prefix. For example,

```sh
spanner-migration-tool schema -prefix=mydb. -source=mysql < my_mysqldump_file
```

will write files `mydb.report.txt`, `mydb.schema.txt`, and
`mydb.dropped.txt`. The prefix can also be a directory. For example,

```sh
spanner-migration-tool schema -prefix=~/spanner-eval-mydb/ -source=mysql < my_mysqldump_file
```

would write the files into the directory `~/spanner-eval-mydb/`. Note
that Spanner migration tool will not create directories as it writes these files.

### Sample dump files

If you don't have ready access to a MySQL database, some example
dump files can be found [here](examples). The file
[cart.mysqldump](examples/cart.mysqldump) contains mysqldump output
for a very basic shopping cart application (just two tables, one for products
and one for user carts). The file [singers.mysqldump](examples/singers.mysqldump) contain
mysqldump output for a version of the [Cloud Spanner
singers](https://cloud.google.com/spanner/docs/schema-and-data-model#creating_a_table)
example.

## Directly connecting to a MySQL database

In this case, Spanner migration tool connects directly to the MySQL database to retrieve
table schema and data. Set the `-source=mysql` and corresponding source profile
connection parameters `host`, `port`, `user`, `dbName` and `password`.

For example, to perform schema conversion, run

```sh
spanner-migration-tool schema -source=mysql -source-profile="host=<>,port=<>,user=<>,dbName=<>"
```

Parameters `port` and `password` are optional. Port (`port`) defaults to `3306`
for MySQL source. Password can be provided at the password prompt.

(⚠ Deprecated ⚠) Set environment variables `MYSQLHOST`, `MYSQLPORT`,
`MYSQLUSER`, `MYSQLDATABASE`. Password can be specified either in the
`MYSQLPWD` environment variable or provided at the password prompt.

Note that the various target-profile params described in the previous section
are also applicable in direct connect mode.