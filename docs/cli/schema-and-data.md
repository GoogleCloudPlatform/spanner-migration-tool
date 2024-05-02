---
layout: default
title: schema-and-data command
parent: SMT CLI
nav_order: 3
---

# Schema-and-data subcommand
{: .no_toc }

This subcommand will generate a schema as well as perform data migration This subcommand can be used to do both POC and minimal downtime migrations. In practice, we have seen this command used
more frequently for POC migrations, in order to get started quickly.

{: .highlight }
The command below assumes that the open-source version of SMT is being used. For the CLI
reference of the gCloud version of SMT, please refer [here](https://cloud.google.com/sdk/gcloud/reference/alpha/spanner/migrate).

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## NAME

    ./spanner-migration-tool schema-and-data - migrate data from a source
        database to Cloud Spanner given a schema

## SYNOPSIS

    ./spanner-migration-tool schema-and-data --source=SOURCE [--dry-run]
        [--log-level=LOG_LEVEL] [--prefix=PREFIX] [--skip-foreign-keys]
        [--source-profile=SOURCE_PROFILE] [--target=TARGET]
        [--target-profile=TARGET_PROFILE] [--write-limit=WRITE_LIMIT]
        [--project=PROJECT] [GCLOUD_WIDE_FLAG ...]

## DESCRIPTION

    Migrate schema and data from a source database to Cloud Spanner.

## EXAMPLES

    To generate schema and copy data to Cloud Spanner from a source PostgreSQL database using pg_dump:

        $ ./spanner-migration-tool schema-and-data --source=postgresql \
            < ~/cart.pg_dump --target-profile='instance=spanner-instance'

    To run a minimal downtime schema and data migration:

        $ ./spanner-migration-tool schema-and-data --source=MySQL \
            --source-profile='host=host,port=3306,user=user,password=pwd,dbN\
        ame=db,streamingCfg=streaming.json' \
            --target-profile='project=spanner-project,instance=spanner-insta\
        nce' --project='migration-project'

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

     --skip-foreign-keys
        Skip creating foreign keys after data migration is complete. This is flag is only valid for POC migrations.

     --source-profile=SOURCE_PROFILE
        Flag for specifying connection profile for source database (e.g.,
        "file=<path>,format=dump").

     --target=TARGET
        Specifies the target database, defaults to Spanner (accepted values:
        Spanner) (default "Spanner").

     --target-profile=TARGET_PROFILE
        Flag for specifying connection profile for target database (e.g.,
        "dialect=postgresql").

     --write-limit=WRITE_LIMIT
        Number of parallel writers to Cloud Spanner during bulk data migrations
        (default 40).

     --project=PROJECT
        Flag for specifying the name of the Google Cloud Project in which the Spanner migration tool
        can create resources required for migration. If the project is not specified, Spanner migration 
        tool will try to fetch the configured project in the gCloud CLI.

     --dataflow-template=DATAFLOW_TEMPLAtE
        GCS path of the Dataflow template. Default value is the latest dataflow template.