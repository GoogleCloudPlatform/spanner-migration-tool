---
layout: default
title: Installation
nav_order: 2
description: "Installing Spanner migration tool"
---

# Installation Guide
{: .no_toc }

You have a couple of options to start using Spanner migration tool, either using a pre-built binary via the gCloud SDK or building from source.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Before you begin

Complete the steps described in
[Set up](https://cloud.google.com/spanner/docs/getting-started/set-up), which
covers creating and setting a default Google Cloud project, enabling billing,
enabling the Cloud Spanner API, and setting up OAuth 2.0 to get authentication
credentials to use the Cloud Spanner API.

In particular, ensure that you run

```sh
gcloud auth application-default login
```

to set up your local development environment with authentication credentials.

Set the GCLOUD_PROJECT environment variable to your Google Cloud project ID:

```sh
export GCLOUD_PROJECT=my-project-id
```

If you do not already have a Cloud Spanner instance, or you want to use a
separate instance specifically for running Spanner migration tool, then create a Cloud
Spanner instance by following the "Create an instance" instructions on the
[Quickstart using the console](https://cloud.google.com/spanner/docs/quickstart-console)
guide. Spanner migration tool will create a database for you, but it will not create a
Spanner instance.

## Installing Spanner migration tool

### Spanner migration tool on gCloud

{: .highlight }
Note: Spanner migration tool on gCloud is currently only supported on the Linux platform. MacOS and Windows are currently not supported.

You can directly run Spanner migration tool from the gCloud CLI instead of building it from source. In order to start using Spanner migration tool via Gcloud, the user can [install the harbourbridge component](https://cloud.google.com/sdk/docs/components#installing_components) of gcloud by executing the below command:

```sh
gcloud components install harbourbridge
```

If you installed the gcloud CLI through the apt or yum package managers, you can also install additional gcloud CLI components using those same package managers. For example, to install with `apt`, run the following:

```sh
sudo apt-get install google-cloud-sdk-harbourbridge
```

Once installed, the Spanner migration tool commands will be available under the `gcloud alpha spanner migration` surface. For example, to start the Spanner migration tool UI, run the following command:

```sh
gcloud alpha spanner migration web
```

The complete CLI reference for the `spanner migration` gCloud surface can be found [here](https://cloud.google.com/sdk/gcloud/reference/alpha/spanner/migration).

Note: Detailed instructions on how to install a new component in gCloud can be found [here](https://cloud.google.com/sdk/docs/install#installation_instructions). 

### Spanner migration tool from Source

{: .highlight }
Building from source is only supporte for MacOS and Linux based platforms

1. Install Go ([download](https://golang.org/doc/install)) on your development machine if it is not already installed, configure the GOPATH environment variable if it is not already configured, and [test your installation](https://golang.org/doc/install#testing).

2. Run the following commands to clone the repository and build it from source:

```sh
git clone https://github.com/GoogleCloudPlatform/spanner-migration-tool
cd spanner-migration-tool
make build
./spanner-migrationt-tool help
```
