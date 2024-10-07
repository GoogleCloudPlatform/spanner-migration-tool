---
layout: default
title: Terraform Templates
nav_order: 4
description: "Terraform templates to run migrations"
---

# Terraform Templates
{: .d-inline-block }

New
{: .label .label-green }

Dataflow templates now contains a repository that provides samples for common scenarios users might have while trying to run a live migration to Spanner using Terraform based orchestration.

Pick a sample that is closest to your use-case, and use it as a starting point, and tailor it to your own specific needs.

## Terraform repositories

1. [Bulk migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/datastream-to-spanner/terraform/samples)
2. [Live migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/terraform/samples)
3. [Sample environment setups](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-common/terraform/samples)

## Commonly used templates & guides

1. [How to run a production migration using a combination of bulk and live migrations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-common/docs/END-TO-END-PRODUCTION-MIGRATION.md)
2. [Bulk migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/terraform/samples/single-job-bulk-migration/README.md)
3. [Live migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/terraform/samples/mysql-end-to-end/README.md)
4. [Sharded bulk migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/terraform/samples/sharded-bulk-migration/README.md)
5. [Sharded live migration using one dataflow job](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/terraform/samples/mysql-sharded-single-df-job/README.md)
6. [Sharded live migration using multiple dataflow jobs](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/terraform/samples/mysql-sharded-end-to-end/README.md)

## Sample structure

Each sample contains the following (and potentially more) files:

1. `main.tf` - This contains the Terraform resources which will be created.
2. `outputs.tf` - This declares the outputs that will be output as part of
   running the terraform example.
3. `variables.tf` - This declares the input variables that are required to
   configure the resources.
4. `terraform.tf` - This contains the required providers and APIs/project
   configurations for the sample.
5. `terraform.tfvars` - This contains the dummy inputs that need to be populated
   to run the example.
6. `terraform_simple.tfvars` - This contains the minimal list of dummy inputs
   that need to be populated to run the example.