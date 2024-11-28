---
layout: default
title: schema command
parent: SMT CLI
nav_order: 1
---

# Schema subcommand
{: .no_toc }

This subcommand can be used to perform schema conversion and report on the quality of the conversion. 
Based on the options discussed further, it helps with:
1. Generate Report on quality of conversion.
2. Generate the Spanner schema in Schema file, which could be manually modified and applied on spanner if required.
3. Generate schema mapping file (`session.json`), which helps the data migration pipeline with the context how the source shcema maps to spanner schema. If required, the schema mapping file can be manually edited (either directly or with the help of SMT web UI). The modified session file can be passed back as **sessionFilePath** parameter to schema sub command if required.
4. If you would like to perform the data migration via spanner migration tool, the session file needs be passed to the [data subcommand](data.md) as the **--session** parameter.
5. Running with `--dry-run` option just generates the report, schema file and session file. In case you also want the generated schema to be automatically applied to spanner, you should run the cli without the `--dry-run` option.

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

    ./spanner-migration-tool schema - migrate schema from a source database
        to Cloud Spanner

## SYNOPSIS

    ./spanner-migration-tool schema --source=SOURCE [--dry-run]
        [--log-level=LOG_LEVEL] [--prefix=PREFIX]
        [--source-profile=SOURCE_PROFILE] [--target=TARGET]
        [--target-profile=TARGET_PROFILE] [--project=PROJECT] [GCLOUD_WIDE_FLAG ...]

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
        nce' --project='migration-project'

## REQUIRED FLAGS

Either `--source-profile` or `--session` must be specified. In case both are specified,
`--source-profile` is not used for schema conversion.

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

     --project=PROJECT
        Flag for specifying the name of the Google Cloud Project in which the Spanner migration tool
        can create resources required for migration. If the project is not specified, Spanner migration 
        tool will try to fetch the configured project in the gCloud CLI.

     --session=SESSION
        Specifies the file that you restore session state from. This file can be generaed using the [schma](schema.md) sub command.

     --source=SOURCE
        Flag for specifying source database (e.g., PostgreSQL, MySQL,
        DynamoDB).