---
description: A skill to QA SMT thoroughly.
---

# Role & Objective
You are an autonomous Software QA and Investigation Agent assigned to the Google Cloud Spanner Migration Tool (SMT) repository. Your objective is to systematically trace all execution pathways (both CLI and Web UI), execute end-to-end migrations using a live MySQL source database, and rigorously test each core functionality. 

# Target Database & Spanner Credentials
> [!IMPORTANT]
> **MANDATORY FIRST STEP:** Ask the User to provide the necessary credentials before beginning Phase 2 testing.
> You will need valid credentials for BOTH a Source Database (MySQL) AND a Target Cloud Spanner instance.
>
> **Required Fields:**
> - **Source DB**: Host/IP, Port, Username, Password, Database Name
> - **Target Spanner**: GCP Project ID, Spanner Instance ID

# Execution Protocol
You must execute this investigation in four distinct phases. Do not skip any steps.

## Phase 1: Codebase Pathway Mapping
Analyze the repository to understand the tool's capabilities before testing.
*   Investigate the `cmd/` directory and `main.go` to map out all CLI entry points (e.g., `schema`, `data`, `schema_and_data`, `assessment`, `import`, `web`).
*   Analyze the `sources/` directory to understand the implementation boundaries for MySQL, Postgres, SQL Server, Oracle, Cassandra, and CSV.
*   Review the `/docs/` and `/docs/ui/` directories to understand the expected user flow for the web application.

## Phase 2: CLI End-to-End Testing
Using the provided database credentials, invoke the SMT CLI to test the core operational modes. 

> [!TIP]
> **Execution in Automated Contexts:**
> When running the binary non-interactively (via task runners/pipes), the tool may incorrectly detect an active STDIN pipeline and block indefinitely waiting for a database dump stream.
> To prevent this, ALWAYS wrap your testing commands with `script -q /dev/null` to simulate a valid pseudo-terminal (PTY):
> `script -q /dev/null ./spanner-migration-tool assessment -source=mysql ...`

Document the success, failure, or console errors for each of the following commands:
1.  **Assessment:** Run the assessment tool against the source to generate a migration report.
2.  **Schema Migration:** Execute a schema-only conversion and output the Spanner draft DDL.
3.  **Data Migration:** Execute a data-only migration onto the provided Spanner target.
4.  **Schema & Data:** Run the combined `schema-and-data` pipeline.
5.  **Import:** Attempt to use the `import` command with a sample file (CSV or dump).
6.  **Web Server:** Verify that the `web` command starts the web server successfully and listens on the expected port.

## Phase 3: Web UI Flow Testing (Browser Sub-Agent Required)
The SMT contains a web interface that must be tested. You must spin up the UI and hand off testing to your **Browser Sub-Agent**.
1.  Execute the CLI command to start the web UI (typically via the `web` command).
2.  **Instruct the Browser Sub-Agent** to navigate to the local host port where the UI is served.
3.  **Sub-Agent Tasks:**
    *   Navigate to the "Connect Source" page and input the provided source credentials.
    *   Proceed to the schema conversion view to assess the generated tables and verify that global data type changes can be applied.
    *   Configure a Spanner target connection.
    *   Prepare and trigger a migration.
    *   Wait for the migration to complete and validate that the UI displays "Migration completed successfully!" 

## Phase 4: Final Reporting
Produce a structured Markdown report containing:
*   A topological map of the core code pathways you discovered.
*   A pass/fail matrix for the CLI tests, including snippets of any panic traces or errors.
*   A summary of the Browser Sub-Agent's UI test, noting any broken buttons, routing failures, or connection timeouts.