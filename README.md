# HarbourBridge: Turnkey Postgres-to-Spanner Evaluation
[![cloudspannerecosystem](https://circleci.com/gh/cloudspannerecosystem/harbourbridge.svg?style=svg)](https://circleci.com/gh/cloudspannerecosystem/harbourbridge)

HarbourBridge is a stand-alone tool for Cloud Spanner evaluation, using data
from an existing PostgreSQL database. The tool ingests pg_dump output,
automatically builds a Spanner schema, and creates a new Spanner database
populated with data from pg_dump.

This tool is currently under development.
