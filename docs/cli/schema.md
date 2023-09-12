---
layout: default
title: schema command
parent: SMT CLI
nav_order: 1
---

# Schema subcommand
{: .no_toc }

This subcommand can be used to perform schema conversion and report on the quality of the conversion. The generated schema mapping file (session.json) can be then further edited using the Spanner migration tool web UI to make custom edits to the destination schema. This session file
is then passed to the data subcommand to perform data migration while honoring the defined
schema mapping. Spanner migration tool also generates Spanner schema which users can modify manually and use directly as well.

{: .highlight }
The command below assumes that the open-source version of SMT is being used. For the CLI
reference of the gCloud version of SMT, please refer [here](https://cloud.google.com/sdk/gcloud/reference/alpha/spanner/migration).

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## NAME

    ./spanner-migration-tool schema - migrate schema from a source database
        to Cloud Spanner

## SYNOPSIS

    ./spanner-migration-tool schema --source=SOURCE [--dry-run]
        [--log-level=LOG_LEVEL] [--prefix=PREFIX]
        [--source-profile=SOURCE_PROFILE] [--target=TARGET]
        [--target-profile=TARGET_PROFILE] [GCLOUD_WIDE_FLAG ...]

## DESCRIPTION

    Migrate schema from a source database to Cloud Spanner.

## EXAMPLES

    To generate schema file for Cloud Spanner GoogleSQL dialect from the source PostgreSQL database using pg_dump:

        $ ./spanner-migration-tool schema --source=postgresql < \
            ~/cart.pg_dump

    To do schema migration with direct connection from source database:

        $ ./spanner-migration-tool schema --source=MySQL \
            --source-profile='host=host,port=3306,user=user,password=pwd,dbN\
        ame=db' \
            --target-profile='project=spanner-project,instance=spanner-insta\
        nce'

## REQUIRED FLAGS

     --source=SOURCE
        Flag for specifying source database (e.g., PostgreSQL, MySQL,
        DynamoDB).

## OPTIONAL FLAGS

{: .highlight }
Detailed description of optional flags can be found [here](./flags.md).

     --dry-run
        Flag for generating DDL and schema conversion report without creating a
        Cloud Spanner database.

     --log-level=LOG_LEVEL
        To configure the log level for the execution (INFO, VERBOSE).

     --prefix=PREFIX
        File prefix for generated files.

     --source-profile=SOURCE_PROFILE
        Flag for specifying connection profile for source database (e.g.,
        "file=<path>,format=dump").

     --target=TARGET
        Specifies the target database, defaults to Spanner (accepted values:
        Spanner) (default "Spanner").

     --target-profile=TARGET_PROFILE
        Flag for specifying connection profile for target database (e.g.,
        "dialect=postgresql").
