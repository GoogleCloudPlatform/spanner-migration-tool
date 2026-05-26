---
layout: default
title: Known Limitations
nav_order: 13
permalink: /known-limitations
---

# Known Limitations
{: .no_toc }

Please refer to the [issues section](https://github.com/GoogleCloudPlatform/spanner-migration-tool/issues)
 on Github for a full list of known issues.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

### Schema Conversion

- Schema Only Mode does not create foreign keys
- Migration of functions and views is not supported
- Schema recommendations are based on static analysis of the schema only


### Access Control

- Spanner migration tool does not support database roles and privileges. If users wish to use Spanner [fine-grained access control](https://cloud.google.com/spanner/docs/configure-fgac) as an IAM principal, then they can manually create database roles, grant the appropriate memberships and privileges to these roles, and grant access to database roles to the IAM principal. Alternatively, users can grant [database-level access](https://cloud.google.com/spanner/docs/grant-permissions#database-level_permissions) to an IAM principal.
