---
layout: default
title: Minimal downtime migrations
nav_order: 7
has_children: true
permalink: /minimal
---

# Minimal downtime migrations
{: .no_toc }

A minimal downtime migration consists of two components, migration of existing data from the database and the stream of changes (writes and updates) that are made to the source database during migration, referred to as change database capture (CDC). Using Spanner migration tool, the entire process where Datastream reads data from the source database and writes to a GCS bucket and data flow reads data from GCS bucket and writes to spanner database can be orchestrated using a unified interface. Performing schema changes on the source database during the migration is not supported. This is the suggested mode of migration for most databases.
