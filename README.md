# HarbourBridge: Turnkey Postgres-to-Spanner Evaluation

HarbourBridge is a stand-alone tool for Cloud Spanner evaluation, using data
from an existing PostgreSQL database. The tool ingests pg_dump output,
automatically builds a Spanner schema, and creates a new Spanner database
populated with data from pg_dump.

This tool is currently under development.
