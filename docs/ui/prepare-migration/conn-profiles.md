---
layout: default
title: Setting up connection profiles
parent: Prepare Migration Page
grand_parent: SMT UI
nav_order: 3
---

# Setting up connection profiles
{: .no_toc }

In case of minimal downtime migration, Spanner Migration Tool needs information about connection profiles which are required by the datastream. There are two connection profiles that need to be set up - **source connection profile** and **target connection profile.**

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Source connection profile

**Source connection profile** is used by datastream to connect to the source database and read the data from the source database. Users can either use an existing source connection profile or create a new connection profile from Spanner Migration Tool by specifying a new name for the connection profile and allowlisting the IPs.

![](https://services.google.com/fh/files/helpcenter/asset-79x00z9xt7.png)

![](https://services.google.com/fh/files/helpcenter/asset-okr3ditazlp.png)

## Target connection profile

**Target connection profile** is used to connect to the GCS bucket where the datastream writes data written to. Users can either use an existing target connection profile or create a new one from Spanner Migration Tool by specifying a new name for the connection profile. Please ensure that the GCS bucket is empty in case you choose an existing connection profile to ensure consistency between source and spanner database. In case the user opts for a new target connection profile, Spanner Migration Tool creates a new GCS bucket with bucket name as the Migration Request ID.

![](https://services.google.com/fh/files/helpcenter/asset-lxybfzd2cpm.png)

![](https://services.google.com/fh/files/helpcenter/asset-ja7bcor0lt8.png)