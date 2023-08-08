---
layout: default
title: SMT UI
nav_order: 5
has_children: true
permalink: /ui
---

# SMT UI
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
- **Session file** - A session file is a snapshot of an ongoing HarbourBridge conversion session. It contains metadata for migration and is structured in JSON format.
- **Interleave** - Spanner's table interleaving is a good choice for many parent-child relationships. With interleaving, Spanner physically co-locates child rows with parent rows in storage. Co-location can significantly improve performance. For more information on interleaving check [here](https://cloud.google.com/spanner/docs/schema-and-data-model#parent-child).
- **Migration Request Id** - A unique identifier generated for each migration request.
- **Synthetic Primary Key** - It is mandatory for a table in spanner to have a primary key. In cases where the primary key is missing in the source database, HarbourBridge generates a new column **synth_id** and populates it with UUID.
- **Metadata database** - A spanner database with the name **harbourbridge_metadata** which is responsible for storing saved sessions from HarbourBridge.

## User Journeys

HarbourBridge provides support for both schema and data migration. For schema migration, HarbourBridge automatically builds a default Spanner schema from the schema of the source database. This schema can be customized using the HarbourBridge schema assistant. After schema customizations the user can then go ahead with the migration wherein they select the mode of migration - schema, data or schema and data and type of migration - poc migration or minimal downtime migration and then execute the migration. After all the details have been specified, a database gets created in Spanner with the customized schema and data is copied from the existing database to Spanner.
