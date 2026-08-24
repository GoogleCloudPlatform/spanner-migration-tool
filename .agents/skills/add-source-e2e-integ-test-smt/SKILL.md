---
name: add-source-e2e-integ-test-smt
description: Skill for independently generating Integration Tests and performing End-to-End QA validation for newly added Spanner Migration Tool database sources.
---
# Skill: SMT New Source Integration and E2E Testing

**Description:** This skill provides detailed instructions for an independent agent to construct integration tests and execute end-to-end full-stack manual testing for newly integrated database sources in the Spanner Migration Tool (SMT). Operating independently blocks confirmation bias from the original implementation agent.

## 1. Prerequisites

### 1.1 Required Input Files
**CRITICAL HALT RULE**: Before writing any tests, check if the user provided ALL of the following: a data-type mapping matrix file, a feature support matrix file, and a testing environment setup path (e.g., `testing_execution.env`). If the user did *not* provide these files and test credentials, you **must STOP execution immediately** and politely ask the user to provide them before proceeding. These files act as your Absolute Ground Truth for assertions. 

### 1.2 Execution Configuration
If the user provided a `testing_execution.env` file in the workspace, it typically contains the following schema:
```env
REMOTE_WORKSPACE_PATH=
REMOTE_EXEC_TEMPLATE=
REMOTE_SYNC_TEMPLATE=
```
You MUST load these variables and route any physical server commands (compilation, backend boot-up, UI server) through the `REMOTE_EXEC_TEMPLATE`.

---

## Phase 1: Database Integration Testing

### Step 1: Implement Permanent Backend Tests (ITs)
1. **Implement Integration Tests (IT)**: You must write and completely implement a comprehensive suite of integration tests inside a new `testing/<database>/integration_test.go` file. Your tests must execute and assert the core backend database extraction CLI workflows, achieving strict testing parity with the existing MySQL ITs. Use exactly the structural mappings found in the user's provided matrix files to build your test validations.

### Step 2: Global Regression Testing
1. **Backend Integration Regressions**: Trigger a workspace-wide `go test ./testing/...` to guarantee the engine did not inadvertently break pre-existing platforms.
2. **UI E2E Regressions**: Navigate to `ui/` and trigger Cypress regression suites (`npm run cypress:run`) to ensure new HTML/TS implementations did not poison other database UI flows.

---

## Phase 2: Exhaustive E2E Validation

Because automated Backend ITs only test the CLI, they **never test the true physical bridge** to the Angular UI. You must act as a QA engineer and manually execute the scenarios across the full live stack using these 4 steps:

### Step 3: Server and Environment Boot-up
1. **Backend**: Compile and boot the `smt-cli` web backend server (`go run main.go web` in the background).
2. **Frontend**: Build and boot the Angular frontend (`npm run build`).
3. **Execution Routing**: To determine *where* and *how* to run the backend and database, you must read the `testing_execution.env` file. You must route all your validation commands, compiler executions, and server setups through the `REMOTE_EXEC_TEMPLATE` strings provided there. Do not attempt to randomly run local commands if a remote template is strictly provided.

### Step 4: Full-Spectrum Database Population
Using the credentials from your test setup, connect directly to the target database shell. Populate the physical DB with every single data type and edge-case table available from the Data-Type mapping matrix (e.g., arrays, proprietary constraints). 

### Step 5: End-to-End Extraction Execution 
Assess your current Agent Tooling capabilities and execute the test:
- **If you possess UI Rendering Capabilities** (e.g., Headless browser tools, visual/DOM orchestration hooks): Execute the true end-to-end testing visually by navigating to `localhost:8080`, rendering the Angular SPA UI directly via your browser tools, and clicking through the direct-connection flow.
- **If you DO NOT possess UI Rendering Capabilities** (e.g., standard text/code LLMs): Programmatically simulate the exact Angular direct-connection UI flow by crafting a strictly typed `curl -X POST` request against the `localhost:8080/connect` endpoint, followed by fetching `localhost:8080/convert/infoschema`.

### Step 6: Verify Edge-Case Masking
Assert that the backend Go-Drivers did not panic during the extraction. Specifically assert that complex precision masking (like parsing `TIMESTAMP(6)` or spatial data wrappers) translated correctly on the real wire and returned a strict `200 OK` on all handlers.

---

## Phase 3: Final Reporting

### Step 7: Compile and Report
1. **Compile**: Run `go build ./...`.
2. **Report**: Create a final markdown artifact (`[database]_testing_report.md`) detailing the validation. Include:
    - Explicit printed test validations documenting the status of your Global ITs and Global Cypress suites.
    - The outcome of your True-Integrated Manual DB Evaluation, **explicitly highlighting** your tooling capabilities and whether you tested the integration natively via UI browser rendering tools, or used the backend `curl` programmatic API simulation approach.
