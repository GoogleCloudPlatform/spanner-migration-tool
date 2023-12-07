---
layout: default
title: User guide
parent: Monitoring
nav_order: 1
---
# Monitoring for Migrations
{: .no_toc }

The Monitoring Dashboards for Migrations helps capture system insights for various components of the migration such as dataflow, datastream, spanner, etc. The dahsboard can be used as a tool for logging 
and diagnostics. For each of these components using measures such as CPU utilization, storage, throughput and more you can track the health and status of your migration. It also helps identify if anything 
is not working as expected and if any of the components need to be scaled up or down. 
Spanner Migration Tool uses the [**Google Cloud Monitoring Service**](https://cloud.google.com/monitoring) to create custom dashboard for each migration.

Each monitoring dashboard contains the following features as seen in the screenshot provided:
1. Top level metrics for an overview of overall health and progress of migration.
2. Five sections for Dataflow, Datastream, GCS Bucket, Pubsub and Spanner for indepth analysis of each component.
3. Time range filter to filter the statistics by time ranges, such as hours, days, or a custom range.
4. For sharded migrations, monitoring dashboards for each shard and an aggregated dashboard for overall monitoring.

<img src="https://services.google.com/fh/files/misc/mon-dashboard-main.png"  style="width:800px;"/>

## Where is my Dashboard?

### UI
The Monitoring Dashboard link can be found under the **Monitoring Dashboards** section on the Prepare Migration page after all the resources have been generated. 

<img src="https://services.google.com/fh/files/misc/mon-dashboard-prep-mig.png"  style="width:500px;"/>

### CLI
The unique name for the dahsboard will be printed on the console. This dashboards can be accessed through cloud monitoring custom dashboards page.

<img src="https://services.google.com/fh/files/misc/dashboard-link-cli.png"  style="width:300px;"/>

<img src="https://services.google.com/fh/files/misc/custom-dshboards.png"  style="width:500px;"/>

<br>

For further details on metrics in the Monitoring Dashboard refer to the links below:
1. [Migration Monitoring Dashboard](./MonitoringMigrationDashboard.md)
2. [Sharded Migration Monitoring Dashbord](./MonitoringMigrationDashboardSharded.md)