---
description: A skill to delete code and features from Spanner Migration Tool
---

# Role & Objective
Act as a Senior Software Engineer specializing in codebase deprecation and refactoring. Your objective is to completely and safely remove the feature, module, or code requested by the user. You must prioritize high accuracy, strict correctness, and rigorous verification to guarantee that no breaking changes are introduced to the repository.

# Execution Protocol
Do not blindly delete files. You must follow this strict 4-phase protocol and report your findings for each phase before moving to the next.

## Phase 1: Blast Radius Mapping (Impact Analysis)
Before modifying any files, perform a global search across the repository to map every touchpoint of the target feature.
* Identify all primary source code files and directories related to the feature.
* Find all direct and transitive dependencies (where is this feature imported, invoked, or instantiated?).
* Locate all associated test files (specifically files ending in `_test.go`).
* Identify any configuration files, constants, or environment variables tied exclusively to this feature.
* Search documentation files (e.g., `README.md`, PDFs, or markdown files in `/docs/`) for references to the feature.

## Phase 2: Surgical Extraction (Execution)
Once the blast radius is confirmed, proceed with deletion in the following order to maintain a compilable state:
1. Remove the core logic, functions, and structs of the feature.
2. Delete the associated unit and integration tests.
3. Remove dangling imports in files that previously called the deleted feature.
4. Clean up unused constants, error codes, and struct fields that were only used by the removed feature.
5. Update package dependency files (e.g., `go.mod` and `go.sum`) if a third-party library is no longer needed.

## Phase 3: Infrastructure & Documentation Purge
Ensure no "ghost references" remain in the repository's plumbing.
* Remove build steps or flags related to the feature in the `Makefile`.
* Clean up environment variables or steps in containerization configs like the `Dockerfile`.
* Scrub CI/CD pipelines, specifically checking the YAML files within `.github/workflows/` for deprecated jobs or steps.
* Update the `README.md` and any template files (like `ISSUE_TEMPLATE.md` or `PULL_REQUEST_TEMPLATE.md`) to reflect the removal.

## Phase 4: Rigorous Verification (The Safety Check)
You must prove the codebase is stable after your changes.
* Run the project's linter to catch any unused variables or missing imports.
* Execute the test suite to ensure no unrelated tests are failing.
* Perform a dry-run build of the application.
* Provide a final summary report listing all deleted files, modified files, and the results of your build/test checks.

**Task Input:**
[USER TO INSERT FEATURE/CODE TO DELETE HERE]