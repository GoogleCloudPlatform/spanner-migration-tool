---
layout: default
title: Schema migration
nav_order: 5
has_children: true
permalink: /schema-migration
---

# Schema migrations via SMT
{: .no_toc }

SMT currently supports performing schema migrations for MySQL, PostgreSQL, SQLServer and Oracle. Certain features of relational databases, especially those that don't map directly to Spanner features, are ignored, e.g. stored functions and procedures, and sequences. Types such as integers, floats, char/text, bools, timestamps, and (some) array types, map fairly directly to Spanner, but many other types do not and instead are mapped to Spanner's `STRING(MAX)`.

SMT supports converting to both GoogleSQL and PostgreSQL [dialects](https://cloud.google.com/spanner/docs) of Spanner.
