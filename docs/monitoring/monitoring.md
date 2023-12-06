# Monitoring for Migrations
Spanner Migration Monitoring for Sharded and Non-Sharded Migrations is now available. The monitoring dahsboard can be accessed through both UI and CLI. Spanner Migration Tool uses the [**Google Cloud Monitoring Service**](https://cloud.google.com/monitoring) to create custom dashboard for each migration.

## Sharded Migration
### UI
1. The Monitoring Dashboard for each shard along with their corresponding shard id and the Aggregated Monitoring Dashboard link can be found under the **Monitoring Dashboards** section on the Prepare Migration page after all the resources have been generated. 
2. Along with this a list with shards and their corresponding dahsboards can be found on the Aggregated Monitoring Dashboard itself.

### CLI
1. The unique name for each dahsboard along with the shard id will be printed on the console.
2. These dashboards can be accessed through cloud monitoring custom dashboards page.
3. Aggregated Monitoring Dashbaord name will also be provided.
### Non-Sharded Migration
### UI
The Monitoring Dashboard link can be found under the **Monitoring Dashboards** section on the Prepare Migration page after all the resources have been generated. 
### CLI
The unique name for the dahsboard will be printed on the console. This dashboards can be accessed through cloud monitoring custom dashboards page.

## Metrics

### Per Shard or Non-sharded Migration Dashboard
The following is a list of important metrics to track during a migration:
- Dataflow Workers CPU Utilisation
- Datastream Throughput
- Datastream Unsupported Events
- Pubsub Age of oldest unacknowledged message
- Spanner CPU Utilisation
- Spanner Storage

Below is an exhaustive list of all metrics in the dashboard and their usage.

#### Dataflow
| Metric | Description | Importance |
| ----------- | ----------- | ----------- |
| Worker CPU Utilization | Shows the CPU Utilization of the 50th, 90th and 100th percentile worker| Used to identify if the pipelines is over or under scaled based on the value of CPU Utilization| 
| Worker Memory Utilization | Shows the Memory Utilization of the 50th, 90th and 100th percentile worker| Used to identify if the health of the pipeline based on the value of Memory Utilization| 
| Worker Max Backlog Seconds | Shows max time required to consume the largest backlog across all stages for each dataflow worker | Used to identify if the pipelines is over or under scaled| 


### Aggregated Dashboard