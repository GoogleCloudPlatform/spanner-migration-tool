---
layout: default
title: Tune Dataflow (Optional)
parent: Prepare Migration Page
grand_parent: SMT UI
nav_order: 6
---

# Tune Dataflow (Optional)
{: .no_toc }

{: .important }

This is relevant especially if you want to run Dataflow inside a VPC.

In case of minimal downtime migration, the dataflow jobs launched by the Spanner Migration Tool can be **optionally** tuned with custom runtime environment variables such as MaxWorkers, NumWorkers, specifying [networks and subnetworks](https://cloud.google.com/dataflow/docs/guides/specifying-networks) etc. Tuning refers to tweaking these parameters to run dataflow is a custom configuration.

## Tuning use cases

SMT by default launches dataflow with a preset configuration. However, this may not be applicable to all use cases. 
Some use cases when the user would want to tweak the jobs are:
- Dataflow machines should run inside a VPC.
- Dataflow and Spanner should run in separate projects for cost tracking.
- Use a custom service account to launch the Dataflow job.
- Apply labels for better cost tracking for the jobs.


{: .highlight }

To tune dataflow, first specify the target database in the 'Configure Spanner Database' step. This enables the configure button for the remaining steps.

![](https://services.google.com/fh/gumdrop/preview/misc/dataflow-form.png)

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

{: .highlight }

SMT exposes the most frequently changed dataflow configurations to the user. Please reach out to us if you have a use-case that is not satisfied by the provided configurations.

### Custom JAR GCS Path
Specify the GCS path of the jar containing custom transformation logic. For custom transformation, specify both custom jar GCS path and fully classified class name. If no custom jar and class name are provided, only the default transformations will be used.

Present under the Custom Transformations section of the form.

### Custom Class Name
Specify the fully classified class name of the class containing custom transformation logic. For custom transformation, specify both custom jar GCS path and fully classified class name. If no custom jar and class name are provided, only the default transformations will be used.

Present under the Custom Transformations section of the form.

{: .highlight }
Specify both the custom class name and custom jar GCS path, or specify neither.

### Custom Parameter
Specify the custom parameters to be passed to the custom transformation logic implementation.

Present under the Custom Transformations section of the form.

### VPC Host ProjectId
Specify the project id of the VPC that you want to use. This is required in order to use private connectivity. By default, this is assumed to be the same as Spanner project. Ensure this is specified if also specifying a network and subnetwork.

If using a shared VPC, a common practice is to have it in a separate project. Ensure this field specifies the correct host project for shared VPC use cases.

Present under the Networking section of the form. 

### VPC Network
Specify the name of the VPC network to use. For private connectivity, specify both the VPC network and subnetwork. If no network and subnetwork is provided, the [default](https://cloud.google.com/dataflow/docs/guides/specifying-networks#specifying_a_network_and_a_subnetwork) network is used.

Present under the Networking section of the form. 

### VPC Subnetwork
Specify the name of the VPC subnetwork to use. For private connectivity, specify both the VPC network and subnetwork. If no network and subnetwork is provided, the [default](https://cloud.google.com/dataflow/docs/guides/specifying-networks#specifying_a_network_and_a_subnetwork) network is used.

Present under the Networking section of the form. 

{: .highlight }

SMT sets the IP configuration based on VPC network and subnetwork. If either network or subnetwork is provided (running inside a VPC), the public IPs are disabled (IPConfiguration is private). If neither are provided, the IP configuration is set to PUBLIC.

### Max Workers
Specify the max workers for the dataflow job(s). By default, set to 50.

Present under the Performance section of the form. 

### Number of Workers
Specify the initial number of workers for the dataflow job(s). By default, set to 1.

Present under the Performance section of the form. 

### Machine Type
The machine type to use for the job, eg: n1-standard-2. Use default machine type if not specified.

Present under the Performance section of the form. 

### Service Account Email
Specify a custom service account email to run the job as. Uses the default compute engine service account if not specified. For more details, click [here](https://cloud.google.com/dataflow/docs/reference/pipeline-options#security_and_networking).

### Additional User Labels
Additional user labels to be specified for the job via a JSON string. Example: { "name": "wrench", "mass": "1kg", "count": "3" }.

### KMS Key Name
Name for the Cloud KMS key for the job. Key format is: `projects/my-project/locations/us-central1/keyRings/keyring-name/cryptoKeys/key-name`. Omit this field to use Google Managed Encryption Keys.

## Preset Flags

These flags are set by SMT by default and <b>SHOULD NOT BE</b> modified unless running Dataflow in a non-standard configuration. To edit these parameters, click the edit button in the form next to the preset flags header.

![](https://services.google.com/fh/gumdrop/preview/misc/preset-flags.png)

### Dataflow Project
Specify the project to run the dataflow job in.

### Dataflow Location
Specify the region to run the dataflow job in. It is recommended to keep the region same as Spanner region for performance. Example: us-central1

### GCS Template Path
Cloud Storage path to the template spec. Use this to run launch dataflow with custom templates. Example: `gs://my-bucket/path/to/template`

Checkout how to build the Datastream To Spanner template [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/d161bc7bdb8234ba1206ee92a1a798e8787ceb45/v2/datastream-to-spanner#datastream-to-spanner-dataflow-template).