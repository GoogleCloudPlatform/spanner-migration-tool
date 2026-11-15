# Cassandra to Cloud Spanner Migration Components

⚠️ **WARNING: This is a standalone component and is not integrated with the spanner-migration-tool. It must be used separately by cloning this repository.**

## Overview

This repository contains components for doing live migrations from Cassandra to Google Cloud Spanner.
## Key Components

### 1. [DataStax ZDM Proxy](https://github.com/datastax/zdm-proxy)
- Acts as an intermediary between your application and databases
- Handles dual writes to both Cassandra and Spanner
- Ensures data consistency during migration

### 2. [Spanner Cassandra Java Client](https://github.com/googleapis/java-spanner-cassandra)
- Runs as a sidecar to ZDM proxy
- Translates CQL (Cassandra Query Language) to Spanner API calls
- Enables Cassandra-compatible applications to interact with Spanner

## Architecture

![Cassandra to Spanner Migration Architecture](https://services.google.com/fh/files/misc/live-cassandra.png)


The migration setup consists of:
- **Origin**: Source Cassandra database
- **Target**: Destination Cloud Spanner database
- **ZDM Proxy**: Manages dual writes and read routing
- **Spanner Cassandra Java Client**: Translates CQL to Spanner API calls
- **Client Application**: Your application that interacts with the databases

## Migration Process

1. **Infrastructure Setup**
   - Configure Spanner database
   - Deploy ZDM proxy with sidecar
   - Establish network connectivity

2. **Application Configuration**
   - Point application to ZDM proxy
   - Verify dual-write functionality on Cassandra and Spanner

3. **Data Migration**
   - Execute [bulk data migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner.md#cassandra-to-spanner-bulk-migration).
   - Monitor progress and performance

4. **Validation**
   - Perform data validation over row counts and row data
   - Reconcile any inconsistencies
   - Refer to the [validation scripts](./validations/) for validating data consistency between source and target clusters.

5. **Cutover**
   - Switch application to direct Spanner connection
   - Decommission proxies

## Prerequisites

- Docker (for development/testing)
- Terraform (for production deployment)
- Google Cloud Platform project with Spanner instance
- Service account with appropriate permissions
- Network connectivity between components

## Getting Started

### Single Instance Testing Setup

This guide helps you set up a single instance of the proxy for development or testing purposes using Docker.

#### Prerequisites

Before you begin, ensure you have:
- Docker installed on your system
- Network connectivity configured so that you can access the following from the machine running the proxy: 
  - Source Cassandra cluster (Test quickly via telnet/cqlsh)
  - Target Spanner instance (Test quickly via `gcloud spanner databases execute-sql`)
- Application Default Credentials configured locally (`gcloud auth application-default login`) with permissions to:
  - Read and write data on the target Spanner database (`roles/spanner.databaseUser`)
  - Write client-side metrics to Cloud Monitoring (`roles/monitoring.metricWriter`)
- A Spanner instance and database already set up
- The Spanner database name matches your Cassandra keyspace name

#### Step 1: Build the image

- Keep the entrypoint.sh in the same directory as the Dockerfile  

- Run the following command to build the local image:

```bash
docker build -t zdm-proxy:latest .
```

#### Step 2: Run the container
1. Authenticate locally with Application Default Credentials (avoiding long-lived service account key files):
```bash
gcloud auth application-default login
```

2. Create a `zdm-config.yaml` file with your configuration. See [sample-zdm-config.yaml](./sample-zdm-config.yaml) for an example.

3. Create a log directory on your host machine to store Java proxy logs:
```bash
mkdir -p ./host-logs
```

4. Run the container with the following command:

```bash
docker run -d -p 14002:14002 \
-v $(pwd)/zdm-config.yaml:/zdm-config.yaml:ro \
-v $HOME/.config/gcloud/application_default_credentials.json:/var/run/secret/adc.json:ro \
-v $(pwd)/host-logs:/app/logs \
-e SPANNER_PROJECT=your-project-id \
-e SPANNER_INSTANCE=your-instance-id \
-e SPANNER_DATABASE=your-database-id \
-e GOOGLE_APPLICATION_CREDENTIALS="/var/run/secret/adc.json" \
-e ZDM_CONFIG=/zdm-config.yaml \
zdm-proxy:latest
```

Replace the following values:
- `your-project-id`: Your Google Cloud project ID
- `your-instance-id`: Your Spanner instance ID
- `your-database-id`: Your Spanner database ID

#### Step 4: Verify the Setup

1. Check if the container is running:
```bash
docker ps
```

2. View the container logs (captures ZDM Proxy logs and initial JVM startup messages):
```bash
docker logs <container-id>
```

3. View the detailed Java Spanner Cassandra Proxy logs:
```bash
tail -f ./host-logs/adapter.log
```

4. Test the connection using cqlsh:
```bash
cqlsh localhost 14002
```

If you can connect successfully, your proxy is ready for testing!

#### Troubleshooting

If you encounter issues:
1. Check the container logs for error messages
2. Verify network connectivity to both Cassandra and Spanner
3. Ensure your service account has the necessary permissions
4. Validate your zdm-config.yaml file format and contents

### Production Setup

For production environments, we provide Terraform templates to automate the deployment of a highly available Cassandra-Spanner proxy cluster. This setup handles the creation of VMs, networking, and proxy configuration automatically.

#### Prerequisites

Before proceeding with production deployment, ensure you have:
- Terraform (`>= 1.3`) installed on your system
- Application Default Credentials configured (`gcloud auth application-default login`) with permissions to:
  - Create and manage Compute Engine VMs and firewall rules
  - Attach a service account to Compute Engine VMs (`roles/iam.serviceAccountUser` on the proxy service account)
  - Create a service account and grant Spanner database, Cloud Monitoring, and Secret Manager IAM bindings (only required when `common_params.service_account_email` is omitted and Terraform creates the dedicated proxy service account)
- Spanner instance and database already configured (and Secret Manager & Cloud Monitoring APIs enabled)
- Origin Cassandra password stored in Secret Manager (if your origin Cassandra cluster requires password authentication):
  ```bash
  printf '%s' 'your-cassandra-password' | gcloud secrets create cassandra-origin-password --data-file=- --project=your-project-id
  ```
- If you bring an existing service account via `common_params.service_account_email` instead of letting Terraform create one, grant it:
  - `roles/spanner.databaseUser` on the target Spanner database
  - `roles/monitoring.metricWriter` on the Spanner project (for built-in client-side metrics)
  - `roles/secretmanager.secretAccessor` on the origin password secret (if used)
- The following files in your deployment directory:
  - `Dockerfile`
  - `entrypoint.sh`
  - `spanner-cassandra-config.yaml`
  - Terraform configuration files (`main.tf`, `variables.tf`, `outputs.tf`, `terraform.tf`, `terraform.tfvars`)

#### Infrastructure Sizing

When planning your production deployment, consider these guidelines:

1. **Machine Type**
   - Recommended: `c2-standard-30`
   - Adjust based on your workload requirements

2. **Number of Proxy Instances**
   - Rule of thumb: 1 proxy instance per 10k QPS of write load
   - Example: For 50k QPS writes, use 5 proxy instances
   - Minimum recommendation: 3 nodes for high availability
   - For workloads under 30k QPS, still maintain 3 nodes minimum

#### Deployment Steps

1. **Prepare Terraform Configuration**

   Update your `terraform.tfvars` file with your variables. Take a look at available variables in `variables.tf`. If you need more customization for zdm proxy or java spanner proxy, update the `variables.tf` and `main.tf` to include the other params.

2. **Initialize and Apply Terraform**

   ```bash
   # Initialize Terraform
   terraform init

   # Review the planned changes
   terraform plan -var-file="terraform.tfvars"

   # Apply the configuration
   terraform apply -var-file="terraform.tfvars"
   ```

#### What Gets Deployed

The Terraform template:
1. Creates a dedicated least-privilege service account (unless `common_params.service_account_email` is supplied) and attaches it to the proxy VMs—no service account key files are created or copied to VMs
2. Creates Container-Optimized OS VMs based on your specified count
3. Sets up firewall rules for proxy communication restricted to `common_params.allowed_source_ranges`
4. Generates individual `zdm-config.yaml` files for each VM with topology settings (without plaintext passwords; the origin password is fetched into tmpfs memory at container startup from Secret Manager)
5. Copies `Dockerfile`, `entrypoint.sh`, `spanner-cassandra-config.yaml`, and `zdm-config.yaml` to each VM
6. Builds and launches Docker containers on each VM using Application Default Credentials from the GCE metadata server

⚠️ **Note**: The template assumes a default network configuration. You may need to modify the Terraform scripts to match your specific network infrastructure requirements.

#### Proxy Topology

The template automatically configures the ZDM proxy topology across multiple VMs using:
- `PROXY_TOPOLOGY_ADDRESSES`: List of all proxy instances
- `PROXY_TOPOLOGY_INDEX`: Unique index for each proxy instance

This ensures proper load distribution and high availability across your proxy cluster.


 






 
