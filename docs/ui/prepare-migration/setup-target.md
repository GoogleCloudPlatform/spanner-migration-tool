---
layout: default
title: Setting up target database
parent: Prepare Migration Page
grand_parent: SMT UI
nav_order: 2
---

# Setting up target database details
{: .no_toc }

In order to create a spanner database and/or migrate data to it, the user needs to specify the target database name, it serves as the name of the spanner database that gets created (or gets wirtten to, depending on the migration mode).

{: .important }
Attempt to perform schema-and-data migrations to a database with a non-empty schema will fail.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

![](../assets/asset-2wx0163g8zc.png)