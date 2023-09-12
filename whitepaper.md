# Spanner migration tool Whitepaper

This whitepaper provides some context for Spanner migration tool (formerly known as HarbourBridge) and describes our plans
and aspirations.

## Initial Version

Our first goal is a stand-alone turnkey tool for Cloud Spanner evaluation, using
data from an existing PostgreSQL database. Decisions for this initial version
are driven by a single criterion: make the tool easy to use with the absolute
minimum of configuration. Put simply, we want
```sh
pg_dump mydb | spanner-migration-tool
```
to "just work" for the majority of users.

To achieve this, the initial version automates every step of the process: we
determine cloud project and Spanner instance, we generate a fresh database name,
we build a Spanner schema and create a new Spanner database with this schema,
and we populate it with data from pg_dump. Moreover, statements and features in
the pg_dump output that don't map directly on Spanner features are simply
ignored.

Many of the decisions we automate, while pragmatic for evaluation, are not
appropriate for production database migration. In particular, schema migration
is not a one-size-fits-all process. Production migrations involve many detailed
tradeoffs and decisions about how types and other features of the source
database should map into the target database. These decisions require context
about system architecture and application usage patterns that usually aren't
fully captured in the source database's schema. For the initial version, we
punted on this issue in the interests of developing a simple turnkey tool.

## Next Steps

While continuing to support the turnkey PostgreSQL evaluation use-case, we plan
to expand to other databases and as well as evolve the Spanner migration tool codebase to
address production database migration needs:

* **Support for other databases.** MySQL would be a logical next step, but we
want to go beyond that and plan for any source database that users might want to
use. To get there, we will need to find a more scalable way to access schema and
data from the source database. While pg_dump was a useful way to get
Spanner migration tool boot-strapped, it requires us to parse the "dump" output of each
source database, which represents significant upfront work and a long-term
maintenance burden.

* **Support for user-guided schema conversion.** This will likely involve
splitting out the schema migration functionality into a separate stand-alone
tool with a UI that guides users through the schema conversion choices,
providing options, advice and links to documentation for each choice.

* **Support for larger databases.** The initial version of Spanner migration tool is only
intended for databases up to a few GB. While it can be used for larger
databases, it is missing key features to robustly support such usage. For
example, our write-path for sending data to Spanner does not appropriately take
into account the capacity of the Spanner instance used and how much of this
capacity we can consume. While we do limit the number of in-progress Apply
operations, this is not user configurable, and we may send data too fast for a
single node instance, but too slow for a large multi-node instance. We should
also support greater parallelism for writing large datasets e.g. multiple
"writer-workers" running together. Moreover, we currently have no support for
restarting the data conversion process.

## End State

Our aspiration is to provide an open-source migration toolkit for Spanner that is:

* **flexible:** composed of modules that can be combined, customized, replaced.

* **general:** supports all source databases that Spanner users require.

* **scalable:** supports small-scale experiments through to large database
  migrations.

* **fully-featured:** e.g. supports offline and online migration.

* **community-driven:** owned-by and developed-for the Cloud Spanner user
  community.

For more details, see [Spanner migration tool open
issues](https://github.com/GoogleCloudPlatform/spanner-migration-tool/issues).

