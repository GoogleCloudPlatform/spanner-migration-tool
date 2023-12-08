---
layout: default
title: web command
parent: SMT CLI
nav_order: 4
---

# Web subcommand
{: .no_toc }

{: .note }
This page just documents how to run the SMT UI. The different elements of the UI are covered in the [SMT UI](../ui/ui.md) section of the documentation.

This subcommand will run the Spanner migration tool UI locally. The UI can be used to perform assisted schema and data migration.

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

    ./spanner-migration-tool web - run the web UI assistant for schema
        migrations

## SYNOPSIS

    ./spanner-migration-tool web [--open] [--port=PORT]
        [GCLOUD_WIDE_FLAG ...]

## DESCRIPTION

    Run the web UI assistant for schema migrations.

## EXAMPLES

    To run the web UI assistant:

        $ ./spanner-migration-tool web

    To run the web UI on a specific port and open it in default web browser:

        $ ./spanner-migration-tool web --port=8000 --open

## FLAGS

     --open
        Open the Spanner migration tool web interface in the default browser. Defaults to false.

     --port=PORT
        The port in which Spanner migration tool will run, defaults to 8080.
