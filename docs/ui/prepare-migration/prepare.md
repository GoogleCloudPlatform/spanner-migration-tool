---
layout: default
title: Prepare Migration Page
parent: SMT UI
has_children: true
nav_order: 5
permalink: /ui/prepare
---

# Prepare Migration Page
{: .no_toc }

Once the user is done with schema modifications, they can go ahead with the next step in migration wherein the database gets created in Spanner with the modified schema and data gets migrated to the new spanner database from the existing source database.

## Migration Modes

There are three supported modes of migration:

- **Schema** - This mode of migration creates a spanner database with modified schema without writing any data to the new spanner database. 
- **Data** - This mode of migration writes data to an existing Spanner database. Please note that for data migration to work the schema of the existing spanner database must match with Spanner Migration Tool's generated spanner schema.
- **Schema and Data** - This mode will create a new spanner database with the modified schema and perform the data migration to the new schema.

## Migration Types

Spanner Migration Tool supports two types migration:

- **POC migration** - Spanner Migration Tool reads data from the source database and writes it to the database created in Cloud Spanner. Changes which happen to the source database during the bulk migration may or may not be written to Spanner. To achieve a consistent version of data, stop writing on the source while migration is in progress. While there is no technical limit on the size of the database, it is recommended for migrating moderate-size datasets to Spanner(up to about 100GB).
- **Minimal downtime migration** - A minimal downtime migration consists of two components, migration of existing data from the database and the stream of changes (writes and updates) that are made to the source database during migration, referred to as change database capture (CDC). Using Spanner Migration Tool, the entire process where Datastream reads data from the source database and writes to a GCS bucket and data flow reads data from GCS bucket and writes to a spanner database can be orchestrated using a unified interface. It is suggested for databases that require minimal downtime and for larger databases(> 100GB). Currently, Spanner Migration Tool provides minimal downtime migration support for **MySQL, Oracle and PostgreSQL** databases.

The **Prepare Migration** page provides a summary of the source and target databases. It also provides users with the options for selecting migration mode and migration type. Once the users select these values they need to set up source and target database details. Each of these is described in detail in the sub-components listed below.
