---
layout: default
title: Quickstart
nav_order: 3
description: "Quickstart using Cloud Shell"
---

# Quickstart
{: .no_toc }

This quickstart shows you how to get started with Spanner migration tool using Google Cloud Shell, you will:

* Create a cloud shell in Google Cloud console and install Spanner migration tool on it.
* Launch the web UI of Spanner migration tool.
* Connect to a source database using IP allowlisting.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Installing Spanner migration tool on Cloud Shell

[GCP Cloud shell](https://cloud.google.com/shell) is an online development and operations environment accessible anywhere with your browser. You can manage your resources with its online terminal preloaded with utilities such as the gcloud command-line tool, kubectl, and more. It is free to run, and can help you get started with Spanner migration tool quickly. Follow the instructions below to get started:

1. Login to Google Cloud console and click on the Cloud shell button on the top right corner.

    ![cloud shell](https://services.google.com/fh/files/misc/cloud_shell.png)

2. This is launch a terminal on Cloud console. Wait for the terminal to load.

    ![provision](https://services.google.com/fh/files/misc/provision.png)

3. Once the terminal is loaded, run the following command:

    ```sh
    sudo apt-get install google-cloud-sdk-spanner-migration-tool
    ```

4. Wait for the installation to complete. Spanner migration tool can be accessed using the following gCloud CLI command:

    ```sh
    gcloud alpha spanner migrate <COMMAND>
    ```

## Launching the web UI for Spanner migration tool

1. In order to launch the web UI for Spanner migration tool, run the following command:

    ```sh
    gcloud alpha spanner migrate web
    ```

    {: .important }
    You will be asked to authorize running this command by providing your Google Cloud credentials. This allows Spanner migration tool to access resources on your behalf.

2. This is launch the web UI in the Cloud shell instance, and show the following message in the terminal logs:

    ```sh
    Starting Spanner migration tool UI at: http://localhost:8080
    ```

3. **Without exiting the Cloud shell command**, click on the "Web preview" icon on the top right corner of the cloud shell terminal, and then click on "Preview on port 8080". This will open the Spanner migration tool UI in a new tab. The web address of the UI would something like `https://8080-cs-<random-string>.cs-<region>-vwey.cloudshell.dev/`.

    ![web preview](https://services.google.com/fh/files/misc/web_preview.png)

    {: .important }
    Keep the Cloud console tab with the cloud shell running open in the background. Do not close this tab!

## Connecting to a source database

{: .important }
This example describes connecting to a CloudSQL source database using IP allowlisting. The list of steps here are generic and can be followed for any source instance.

1. In a new tab on the Cloud shell terminal, run the following command to fetch the IP address of the Cloud shell instance

    ```sh
    curl ifconfig.me
    ```

2. Copy the returned IP address. Configure the IP address in the IP allowlisting section of the CloudSQL instance. Follow [this guide](https://cloud.google.com/sql/docs/mysql/configure-ip#add) to configure the IP allowlisting.

3. Go back to the Spanner Migration Tool web UI launched earlier. Follow steps in the [connect to source](./ui/connect-source.md) page to connect to your source database.

{: .important }
The same set of steps can be followed for any database which is accessible from Cloud shell. **Please note** this can not be used for databases within a VPC. If you want to connect to your database from within a VPC, we recommend
following [this guide](https://cloud.google.com/sql/docs/mysql/configure-private-services-access). Note that this approach will not use Cloud shell to run Harboourbridge. Instead, follow the [installation](https://googlecloudplatform.github.io/spanner-migration-tool/install.html#installing-spanner-migration-tool) to install Spanner migration tool on the compute instance in your VPC.
