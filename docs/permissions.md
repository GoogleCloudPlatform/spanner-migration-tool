---
layout: default
title: Permissions and Connectivity
nav_order: 5
description: "Permissions and connectivity required to run SMT"
---

# Permissions & Connectivity
{: .no_toc }

- **Connectivty**: Since both Spanner migration tool and the underlying GCP services talk to the source database for schema and data migration, certain pre-requisite connectivity configurations are required before using the tool.
- **Permissions**: Spanner migration tool (SMT) runs in the customers GCP account. In order to orchestrate migrations, SMT needs access to certain permissions.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Connectivity

### API enablement

1. [Make sure that billing is enabled for your Google Cloud project](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled#gcloud).
2. Google Cloud Storage apis are generally enabled by [default](https://cloud.google.com/service-usage/docs/enabled-service#default). In they have been disabled, you will need to enable them.

   ```sh
   gcloud services enable storage.googleapis.com
   ```

### Configuring connectivity for `spanner-migration-tool`

In order for SMT to read the information schema from the source database, ensure that the machine where you run `spanner-migration-tool` is allowlisted to connect to the source database.
In generic terms (your specific network settings may differ), do the following:

1. Open your source database machine's network firewall rules.
2. Create an inbound rule.
3. Set the source ip address as the ip address of the machine where you run the `spanner-migration-tool`.
4. Set the protocol to TCP.
5. Set the port associated with the TCP protocol of your database.
6. Save the firewall rule, and then exit.




## Permissions

The Spanner migration tool interacts with many GCP services. Please refer to this list for permissions required to perform migrations.

### Spanner

The recommended role to perform migrations is [Cloud Spanner Database Admin](https://cloud.google.com/spanner/docs/iam#spanner.databaseAdmin).

The full list of required [Spanner permissions](https://cloud.google.com/spanner/docs/iam) for migration are

```sh
spanner.instances.list
spanner.instances.get

spanner.databases.create
spanner.databases.list
spanner.databases.get
spanner.databases.getDdl
spanner.databases.updateDdl
spanner.databases.read
spanner.databases.write
spanner.databases.select
```

Refer to the [grant permissions page](https://cloud.google.com/spanner/docs/grant-permissions) for custom roles.


### GCS

Grant the user **Editor role** to create buckets in the project.

### GCE

Enable access to Spanner using [service accounts](https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances).



