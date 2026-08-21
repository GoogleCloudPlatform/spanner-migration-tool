---
name: add-source-support-smt
description: This skill provides comprehensive architectural instructions for agents to safely implement entirely new database source engines into the Spanner Migration Tool (SMT). It depends on user-provided data-type and feature mapping matrices for adding source support on SMT.
---
# Skill: Add New Database Source to Spanner Migration Tool (SMT)

**Description:** This skill provides comprehensive, step-by-step instructions for an agent or LLM to implement end-to-end support for a new database engine in the Spanner Migration Tool.

## Overview
Adding a new database requires two main phases:
1. **Backend Engine**: Implementing the logic to read the source schema and translate its data types to Cloud Spanner types.
2. **End-to-End Integration**: Wiring the new engine into SMT’s connection profiles, API routes, telemetry, protobuf state, and Angular UI.

> [!TIP]
> **Incremental Validation (Fail Fast)**: Always run `go build ./...` and `go test ./...` incrementally after implementing new components, rather than waiting for Phase 3. This catches test suite panics (like forgetting `flag.Parse()` calls before `testing.Short()`) before they cascade into the final deliverable!

---

## 1. Prerequisites

### 1.1 Required Input Files
**CRITICAL HALT RULE**: Before writing any code, check if the user provided ALL of the following: a data-type mapping matrix file, a feature support matrix file, and a testing environment setup path (e.g., `testing_execution.env`). If the user did *not* provide these files and test credentials, you **must STOP execution immediately** and politely ask the user to provide them before proceeding. If the user does not have the mapping files, advise them to formally execute the [source-baseline-matrix-research-skill](.agents/skills/source_research_helper/SKILL.md) to generate baseline mapping files, upon which the user can perform their own research to update and achieve the final mapping files. Do not proceed blind; physical End-to-End validation is strictly required. Do not create your own files as the user's provided files are your Absolute Ground Truth.
**Analyze input files**: Read the Mapping Matrix (containing GoogleSQL and PostgreSQL Dialect mapping structures, including both Default and Alternative assignments) and the Feature Matrix (outlining what system features the target database supports).

**Sample Structures and Header Validation**: You **MUST** strictly validate that the column headers in the user's provided matrices identically match the structural header layouts found in the official SMT MySQL samples. If the user's matrices are missing required columns (e.g., missing specific PostgreSQL edge case columns), you must halt and require the user to structurally fix their inputs using these templates:
* **Data-Type Mapping Matrix Sample**: `https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/.agents/skills/source_research_helper/sampleOutput/mysql_datatype_mapping_matrix.csv`
* **Feature Mapping Matrix Sample**: `https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/.agents/skills/source_research_helper/sampleOutput/mysql_feature_mapping_matrix.csv`

### 1.2 Execution Configuration
The agent must look for a `testing_execution.env` file in the workspace root with the following schema:
```env
REMOTE_WORKSPACE_PATH=
REMOTE_EXEC_TEMPLATE=
REMOTE_SYNC_TEMPLATE=
```
You MUST load these variables and route any physical server commands (compilation, backend boot-up, UI server) through the `REMOTE_EXEC_TEMPLATE` by replacing `<COMMAND>` with your desired command.

---

## Phase 1: Backend Extraction Engine

> [!IMPORTANT]
> **Use MySQL as your Architectural Reference**: Whenever you implement a new folder or file, study how MySQL implemented it to understand the required Go structs and SMT API contracts. You do not need absolute functional parity (especially for non-relational databases), but you must use the MySQL directory as your primary learning model for how to satisfy the internal SMT Go interfaces.

### Step 1: Implement the Core GUI Schema Reader (`sources/<database>/infoschema.go`)
Create a new directory (e.g., `sources/sqlite/`) and implement the `common.InfoSchema` interface. This interface populates the interactive UI flow.
1. **Target Matrix Categories**: From the Feature Mapping Matrix, you **must only** extract features from the `Storage` (Tables, Columns), `Constraints` (Foreign Keys, Primary Keys, Check constraints), `Indexes` (Secondary Indexes), and `Identity` (Sequences) categories. **CRITICAL RULE**: If a sub-category or feature is explicitly marked as `Unsupported` in the Feature Mapping Matrix, you MUST explicitly exclude it from extraction so the engine does not improperly ingest restricted definitions!
2. **Unsupported Spanner Features**: If a generally supported feature applies to a table but violates a Spanner rule (e.g., partial Indexes), you must still write the SQL to fetch them, map them to their parent table, and attach an `internal.SchemaIssue` warning so it correctly flags the user on the UI's warnings tab!
3. **Data Extraction Edge Cases**: Implement `GetRowCount` and `GetRowsFromTable`. If the database has binary types that cannot be fetched natively (e.g., Spatial types, LOB locators), wrap those columns in appropriate conversion functions (e.g., `ST_AsText(col)`) inside your `SELECT` queries.

### Step 2: Implement the Assessment Engine Reader (`assessment/sources/<database>/infoschema.go`)
SMT runs a completely independent static Risk Assessment report system. You must implement `assessment/sources/common.go` (`InfoSchema`).
1. **Target Matrix Categories**: From the Feature Mapping Matrix, you **must only** focus on extracting the "Other" architectural objects missing from Step 1: `Views`, `Stored Procedures`, `Functions`, and `Triggers`.
2. **Implementation Flow**: Write the dialect-specific SQL to fetch these exact objects, and populate SMT's `TriggerAssessmentInfo`, `FunctionAssessmentInfo`, `ViewAssessmentInfo`, etc. structs. This guarantees the Risk Assessment report accurately flags unsupported lines of code.

### Step 3: Implement the Translator (`toddl.go`)
Implement the `common.ToDdl` interface, primarily the `ToSpannerType` method, adhering strictly to your Mapping Matrix ground truth.
1. **Dual-Dialect Generation**: Because Spanner supports two dialects, your implementation must explicitly branch (e.g., by evaluating the `dialect` flag). You must implement distinct translation logic for both the GoogleSQL mappings and PostgreSQL Dialect mappings documented in the user's mapping file.
2. **Dynamic Regexp Precision Stripping**: Databases often serialize precision details directly into the type payload (e.g. `TIMESTAMP(6)`). The translator *must* preemptively use a regex to strip numerical strings before routing the base type to internal switch evaluators to prevent translation panics!
3. **Type Mapping**: Implement the matrices parsed in Section 1.1. **CRITICAL RULE**: You must structurally map *every single* native type provided in the matrix precisely as it appears in the "Default Spanner Type" column across both internal dialects! Do not mock or truncate the logic. **Do NOT generate independent fallback defaults unless explicitly mandated by the Matrix.** Ensure you handle parsing for precisions, lengths, and arrays accurately.
4. **Warnings**: If a conversion is not 1-to-1 (e.g., mapping a larger integer to an Int64, or varying timestamp precisions), attach the appropriate `internal.SchemaIssue` (e.g., `internal.Widened`, `internal.PossibleOverflow`, `internal.Datetime`).
5. **Unsupported Types**: For proprietary types with no Spanner equivalent, **do NOT crash**. Fall back to `ddl.String` and attach the `internal.NoGoodType` warning.
6. **Alternative Types (spType)**: The `spType` parameter to `toSpannerTypeInternal` represents a user-requested alternative Spanner type (e.g., from the UI). You **MUST** implement nested switch statements for every type to explicitly evaluate coercions (e.g., casting `INT64` to `STRING`) and return `internal.Widened` where applicable. **CRITICAL**: Do NOT simply ignore `spType` and return the default type, as this permanently breaks the UI's Type Override Dropdown!
7. **Auto-Generation**: Implement `GetColumnAutoGen` to translate the database's auto-increment mechanisms (like sequences or identity columns).

### Step 4: Document Unsupported Features
1. **Feature Limits**: Open `internal/mapping.go`. If your new database does not support certain advanced migration features (like foreign key `ON DELETE` or `ON UPDATE` actions), you must document it in this file by adding the database to the comments and logic handling unsupported sources.

---

## Phase 2: End-to-End Integration (The "Glue Code")

### Step 5: Protobufs and Constants
1. **Constants**: Open `common/constants/constants.go` and add a new constant representing your database driver.
2. **Protobufs**: Open `proto/migration_data.proto` and assign a new enum ID for the database within `MigrationData_Source`. **CRITICAL RULE**: Do NOT manually edit the generated `proto/migration/migration_data.pb.go` file to match this enum. Modifying generated `.pb.go` structs without using `protoc` directly breaks Go's internal checks and will cause fatal runtime panics (`panic: non-contiguous repeated field`). If you cannot run `protoc`, route new database connections to use `MigrationData_SOURCE_UNSPECIFIED` for telemetry tracking instead.

### Step 6: Connection Profiles (`profiles/` module)
This defines how SMT connects to the database. You must handle the specific connection paradigm of your source.
1. **Define the Profile Struct**: Open `profiles/source_profile.go`. Add a struct representing your connection parameters. 
   - *Network Databases* (e.g., PostgreSQL): You will need Host, Port, User, Password, DBName.
   - *File Databases* (e.g., SQLite): You will only need a FilePath.
2. **Implement Connection Parser**: In the same file, implement the `NewSourceProfileConnection...` method. Throw clear `fmt.Errorf` errors if required fields are missing. **CRITICAL**: When parsing maps, always ensure string parameters are not blank (`value != ""`) in addition to checking dictionary key existence. Add your type to the `SourceProfileConnection` wrapper and the master switch statement.
3. **Build the DSN**: Open `profiles/common.go`. Implement a method (e.g., `getSQLITEConnectionStr`) that builds the specific Go driver Data Source Name string. Add it to the main `GetConnectionStr` switch case.
4. **Profile Tests**: Open `profiles/source_profile_test.go` and add unit tests validating your connection parser.
5. **DSN Tests**: Open `profiles/common_test.go` and add unit tests validating your DSN builder.

### Step 7: Core Web APIs and Telemetry
1. **Telemetry**: Open `common/metrics/metrics.go` and add your database to `getMigrationDataSourceDetails` so metrics are categorized correctly.
2. **Driver Registration**: Open `conversion/get_info.go`. Import the standard Go driver for your database. Add a new `case` to `GetInfoSchema` to implement `sql.Open` and return your `InfoSchemaImpl`.
3. **Conversion Engine**: Open `conversion/conversion.go` and `conversion/conversion_helper.go`. Add your database to the `switch` cases in `SchemaConv`, `DataConv`, and connection string builders. **CRITICAL**: Explicitly add your logic to the `ConnectionConfig` router; if your database is a non-SQL HTTP source that lacks a Data Source Name string, ensure you return an explicit empty string bypass there to avoid falsely triggering a "driver not supported" fallback error. Update tests in `conversion/conversion_test.go`.
4. **Web Routing**: Open `webv2/web.go`. Add a `case` for your driver inside `createDatabaseConnectionString` to route the UI request to the profile builder.
5. **Web Routing Tests**: Open `webv2/web_test.go`. Add corresponding unit tests to verify your new route returns the correct connection constants.
6. **Schema API**: Update `webv2/api/schema.go` and `webv2/utilities/get_type.go` to expose your type translator to the frontend. **CRITICAL**: You must manually populate `[database]TypeMap` and `[database]DefaultTypeMap` inside `initializeTypeMap()`. If you skip this, the UI dropdowns will silently render entirely blank!
7. **Web Helpers**: Update `webv2/helpers/helpers.go` to include your database in `GetSourceDatabaseFromDriver`, and update `helpers_test.go`.

### Step 8: Frontend UI (Angular)
The UI needs explicit configuration to show your new source. Open the frontend components and search for lists like `dbEngineList` or `dbEngines` to append your database.
1. **`ui/src/app/app.constants.ts`**: Add your database to the `SourceDbNames` enum and any feature support lists (like `identitySupportedDbs`).
2. **`ui/src/app/utils/utils.ts`**: Update `extractSourceDbName` so the UI correctly parses the backend database name.
3. **`ui/src/app/components/load-session/load-session.component.ts`**: Add to the dropdown lists.
4. **`ui/src/app/components/direct-connection/direct-connection.component.ts`**: Add to the direct connection form array.
5. **`ui/src/app/components/prepare-migration/prepare-migration.component.ts`**: Update the `ngOnInit` conditions that check `res.DatabaseType` to ensure your database is recognized and allowed to proceed.
6. **`ui/src/app/components/home/home.component.html`**: Update the landing page text description to explicitly list your new database as a supported source.
7. **`ui/src/app/components/object-detail/object-detail.component.ts`**: Update the conditional rendering logic (e.g., `if (this.srcDbName == ...)`) to ensure advanced object details are displayed correctly for your source.
8. **`ui/src/app/components/add-new-column/add-new-column.component.ts`**: Add your database to `autoGenSupportedDbs` if it supports auto-increment columns.
9. **Frontend Unit Tests**: Update corresponding `.spec.ts` files (e.g., `utils.spec.ts`, `load-session.component.spec.ts`, `direct-connection.component.spec.ts`) and Cypress E2E tests (`ui/cypress/e2e/spec.cy.ts`) to validate the new constants and dropdowns.

### Step 9: Dynamic Future-Proof Search (Hybrid Approach)
1. **Verify Architectural Changes**: Because SMT is actively developed, new Angular components or backend routers may have been added since this skill was written. Run a workspace-wide search (`grep_search` or similar) for usages of `constants.MYSQL` (in the Go backend) and `SourceDbNames.MySQL` (in the Angular frontend).
2. **Implement Discovered Files**: Review the search results. If you find any file using the MySQL constant that is *not* covered in the checklist above, you must implement the same logic for your new database. This guarantees 100% feature parity even if the architecture evolves.

---

## Phase 3: Exhaustive E2E Validation

### Step 10: Write Permanent Backend Unit Tests (UTs)
1. **Data-Driven Unit Tests (UT)**: Stop writing tautological unit tests that mistakenly mirror broken internal implementation files. You **MUST** implement strict testing dynamically verifying that every single mapped output natively equals the constraints explicitly defined in both the Feature Mapping Matrix and the Data-Type Mapping Matrix! Rigorously implement data-driven coverage in `sources/<database>/toddl_test.go` and mock scraping metadata via `sources/<database>/infoschema_test.go`. Ensure explicit tests exist for backend Web API endpoints (like asserting `/typemaps` JSON renders correctly).
2. **Integration Tests & E2E**: You must **NOT** implement Integration Tests (ITs) or execute E2E Validation yourself. To avoid confirmation bias (tautological testing), integration tests must be written by a completely independent agent strictly examining the mapping files from the outside-in.

---

## Phase 4: Finalization and Handoff

### Step 11: Compile and Handoff
1. **Compile**: Run `go build ./...`. Ensure there are no undeclared variables or unused imports.
2. **Report**: Create a final markdown artifact (`[database]_implementation_report.md`) detailing the implementation. Include:
    - Clickable file links of all files created and modified.
    - A Markdown table showing the Type Mapping Matrix (Source Type -> Spanner Type -> Warnings).
3. **Independent Testing Handoff**: Explicitly and unequivocally instruct the user that the backend extraction implementation is complete, but it **requires independent integration testing**. Advise the user to execute the `add-source-e2e-integ-test-smt` skill via a new agent to structurally build the Integration Tests and perform the Live QA E2E validation.
