---
layout: default
title: SMT UI
nav_order: 5
has_children: true
permalink: /ui
---

# Spanner migration tool UI
{: .no_toc }

Spanner migration tool UI provides a unified interface for the migration wherein it gives users
the flexibility to modify the generated spanner schema and run end to end migration from
a single interface. It provides the capabilities of editing table details like columns,
primary key, foreign key, indexes, etc and provides insights on the schema conversion
along with highlighting important issues and suggestions.

This documentation will describe various elements of the UI and how to use them.

## Terms/Terminology

- **Connection Profile** - A representation of a source or destination in terms of the connectivity information required to connect to it (e.g. hostname, user, etc).
- **Migration job** - A migration job represents the process of migrating schema and/or data from source to destination, including managing the dump of historical data, replicating data continuously, monitoring and error handling
- **Session file** - A session file is a snapshot of an ongoing Spanner Migration Tool conversion session. It contains metadata for migration and is structured in JSON format.
- **Interleave** - Spanner's table interleaving is a good choice for many parent-child relationships. With interleaving, Spanner physically co-locates child rows with parent rows in storage. Co-location can significantly improve performance. For more information on interleaving check [here](https://cloud.google.com/spanner/docs/schema-and-data-model#parent-child).
- **Migration Request Id** - A unique identifier generated for each migration request.
- **Synthetic Primary Key** - It is mandatory for a table in spanner to have a primary key. In cases where the primary key is missing in the source database, Spanner Migration Tool generates a new column **synth_id** and populates it with UUID.
- **Metadata database** - A spanner database with the name **spannermigrationtool_metadata** which is responsible for storing saved sessions from Spanner Migration Tool.

## UI Components

Spanner migration tool UI has the following components:

- **[Connect to Spanner](./connect-spanner.md)** - This contains the ability to configure the GCP projectId and the spanner instanceId that will be used in the migration.
- **[Connect to Source Page](./connect-source.md)** - This page can be used to configure source of data in the Spanner migration tool UI.
- **[Schema Conversion Workspace](./schema-conv/schema-conv.md)** - This page can be used to make schema conversion changes from source to Spanner. This page will help you visualise how your schema would look like in Spanner, and also provide issues/warnings/suggestions based on the automated analysis of your schema.
- **[Prepare Migration Page](./prepare-migration/prepare.md)** - This page is used to configure the migration. It asks for details such as the mode (`schema`/`data`/`schema-and-data`) and the type (`poc` or `minimal downtime`) of migration and accordingly requests for configuration input (e.g destionation `databaseName`, Datastream connection profiles etc.)
