---
name: source-baseline-matrix-research-skill
description: >
  Skill for helping achieve datatype and feature mapping matrix for any source against spanner. It does research and provide baseline mapping matrix on which user can expand upto.
---

# New Source DataType Mapping Research Skill

This skill instructs an AI Research Agent to discover, analyze, and compile an exhaustive, dialect-wide data type mapping matrix from a new database source dialect to Spanner standard SQL dialects.

This generated mapping file will act as the "Ground Truth" artifact during the Datatype Integration Testing phases.

---

## 1. Goal
Programmatically extract and research a comprehensive list of all native datatypes supported by the target database dialect, determine the native Spanner standard SQL mappings for them (both primary and alternative paths), and generate an exhaustive logical mapping matrix spreadsheet/markdown document for user approval.

---

## 2. Research & Extraction Process

> [!IMPORTANT]
> **Mandatory Tool-Based Raw Extraction Requirement**:
> You MUST NOT rely on your internal LLM memory or knowledge base to list data types, as this inevitably suffers from token limits and summarization biases. Instead, you MUST write and execute a script (e.g., Python/Bash) or use web/file-reading tools to programmatically scrape/extract EVERY single data type from the official database documentation.
>
> **Anti-Summarization Rule**: You are strictly forbidden from summarizing or limiting the list to "common" migration types. You must actively search for and include the database's "long tail" data types.
> **Do not pre-filter types**: Do not silently omit obscure, legacy, or system data types. You MUST include them in the mapping matrix and mark them as Unsupported (Scenario D) with a rationale if they are not supported in spanner. Exhaustive means exhaustive.

#### 2.1 Programmatic Raw Extraction (First Principles Checklist)
- Write and execute a script (or use available web/file tools) to scrape and extract the complete, unfiltered list of *all* native datatypes and their official aliases directly from the official database documentation.
- **Collection & UDT Mandate**: You MUST actively search for and explicitly extract all Object-Relational Collection Types and hierarchical User-Defined Types. Do not restrict your extraction exclusively to simple scalar primitives.
- Save this exhaustive list of extracted data types to a local text file named `raw_types_extraction.txt`. This file forms the baseline absolute ground truth checklist.

#### 2.2 Hybrid Extraction Strategy: Undocumented/Experimental Research Pass
- The official documentation is often incomplete. After establishing the programmatic baseline, you MUST perform broad LLM research (using web search, code search, or internal knowledge) to discover hidden, undocumented, deprecated, or experimental data types.
- You must explicitly search across these avenues:
  1. **Release Notes & Changelogs** (for newly added but undocumented types).
  2. **GitHub Issues & Pull Requests** (for edge-cases developers complain about).
  3. **Driver Source Code** (JDBC, ODBC, or official language clients, which often reveal internal type enums like `Types.OTHER`).
  4. **Database Engine Source Code** (Internal type enumerations or system catalogs like `pg_type` / `information_schema.columns`).
  5. **Migration Ecosystem** (Debezium, Fivetran, Kafka Connect connector docs often document bizarre edge-case types).
  6. **StackOverflow & Developer Blogs**.
- If you discover any types from these avenues that are not in your `raw_types_extraction.txt` baseline, you MUST append them to the file.

#### 2.3 Programmatic Validation
- Read the final augmented `raw_types_extraction.txt`. You MUST generate exactly one row in the mapping table for every single line in that file.
- Reconcile and group the types into standard categories:
  - Numeric Types
  - Character/String Types
  - Binary/LOB Types
  - Temporal Types
  - Boolean Types
  - Array/Collection Types
  - JSON/XML Types
  - Geometric/Spatial Types
  - Network Address Types
  - UUID/Identifier Types
  - Range & Multirange Types
  - User-Defined & Custom Types
  - System/Internal Types

#### 2.4 Discover Alternative Mappings in Conversion Code & Public Dialects
- Analyze the Dataflow template's (repository: https://github.com/GoogleCloudPlatform/DataflowTemplates) generic conversion classes (e.g., `GenericRecordTypeConvertor.java` and `AvroToValueMapper.java`) to understand what alternative mappings are supported via user overrides.
- **Leverage Public Mapping References (Cheat Sheets):** Research how mature open-source projects or JDBC drivers map the type:
  - **Hibernate Dialects**: Search for the target database's Hibernate Dialect class.
  - **JDBC Driver Type Mappings**: Check how the JDBC driver handles the type (e.g., mapping IPADDRESS to binary format).
  - **Beam SchemaUtil**: Check `org.apache.beam.sdk.io.jdbc.SchemaUtil` to see how Beam maps standard JDBC types.
- For every supported type, identify **ALL viable, structurally valid alternative Spanner mappings** (Scenario B) based on this research. Do not limit to just one alternative; list all possible fallback types (e.g., `INT64`, `STRING`, `FLOAT64` for numerical fallbacks).

## 3. Creating the Logical Type Mapping Table
Construct a markdown table containing exactly these columns:
- `Mapping Category`
- `Source Database Type / Alias`
- `Is Source Datatype Supported as PK?`
- `Spanner GoogleSQL Default Datatype`
- `Spanner GoogleSQL Default Datatype If Column is PK`
- `Spanner GoogleSQL Alternative Datatypes`
- `Spanner PostgreSQL Default Datatype`
- `Spanner PostgreSQL Default Datatype If Column is PK`
- `Spanner PostgreSQL Alternative Datatypes`
- `Datastream Support Status`
- `Edge Cases for Smoke Test`
- `Notes`

#### Column Definitions & Constraints
- **Mapping Category & Source Database Type / Alias:** Group the types logically and list the exact native type or alias from the source database.
- **Is Source Datatype Supported as PK?:** You MUST explicitly verify whether the source database natively allows this exact datatype to be designated as a Primary Key (or part of a composite primary key) within the source architecture. Output 'Yes' or 'No'.
- **Spanner GoogleSQL & PostgreSQL Default Datatype:** Identify the safest, most logical 1-to-1 default mapping for *both* dialects independently (observing syntax limits like `NUMERIC` vs `PG.NUMERIC`).
- **The "Default Datatype If Column is PK" Fallback Logic:** You MUST explicitly execute a web search against the official Google Cloud documentation to verify if your proposed Spanner equivalent datatype is legally permitted in a Primary Key or Key Column. You must verify this for BOTH the GoogleSQL and PostgreSQL dialects independently because they deliberately contain divergent constraints. If the source datatype CAN be used as a PK, but your proposed Spanner Default Datatype is evaluated as illegal for a PK based on the official documentation, you MUST output a safe Spanner fallback type here (chosen from your alternatives). If the default is evaluated as safe for PKs, simply mirror the default type here. If the source datatype CANNOT be used as a PK natively, simply output 'N/A' (Not Applicable) for this column.
- **Spanner GoogleSQL & PostgreSQL Alternative Datatypes:** List ALL viable, structurally valid alternative mappings for both dialects.
- **Datastream Support Status:** Because our live templates rely on Datastream, you MUST explicitly verify each datatype against the official Google Cloud Datastream replication support documentation. If Datastream cannot currently pull this source datatype natively, we cannot migrate it in live pipelines. Mark this explicitly as 'Supported' or 'Unsupported' based purely on Datastream's capabilities.
- **Edge Cases for Smoke Test:** You MUST explicitly perform research to definitively understand the specific architectural limits of the source database datatype first (evaluating the strict max precision bounds of a dialect's temporal type, or the max-byte boundary of its numeric type). Based purely on those discovered source limits, you MUST output an array of the *literal, raw test values* themselves that represent the extreme boundaries and edge cases. Do not write text instructions or hypotheses. These values will be extracted and directly fed into a downstream automated test runner. You MUST provide these test values for ALL datatypes, even if the type is marked as 'Unsupported' by Datastream or Spanner natively, so the testing pipeline can validate rejection handling or explicit workaround logic.
- **Notes:** You MUST explicitly document in this column whether the Spanner mapping is a straightforward native 1-to-1 translation, OR document the precise workaround tricks required to achieve the mapping (e.g., storing a geometric shape as a JSON string).

*Note: Do not put citations or sources inside the table itself. Instead, create a dedicated **"Citations & Sources"** section at the bottom of the artifact to list all URLs, JDBC docs, internal repos, and ecosystem tools you referenced during your research.*

## 4. Feature Support Discovery

> [!IMPORTANT]
> **Anti-Hallucination Rule**: You are strictly forbidden from relying on your internal LLM memory to evaluate feature support. You MUST use search tools to locate official documentation for every single feature before mapping it.

### 4.1 The Master Validation List & Strict Categorization Taxonomy
You must explicitly research and evaluate the source database for all of the critical architectural features listed below. 

**Strict Category Names:** When populating the `Category` column in the matrix, you MUST strictly use exactly these 5 bolded category strings for the items listed beneath them. Downstream automated templates rigidly parse this output expecting these exact category strings. 
However, **you are NOT restricted to only these categories.** You must research other core database functionalities (e.g., Stored Procedures, Views, Row-Level Security) or proprietary edge cases, map them to `Others` category, and append them to the report alongside these mandatory minimums.

1. **Constraints**: Cascading Foreign Keys, Deferred Constraints, Check/Unique Constraints.
2. **Indexes**: Partial/Filtered Indexes, Function-Based Indexes, Bitmap Indexes, Full-Text/Vector Indexes.
3. **Transactions**: Transaction Isolation Levels (vs Strict Serializability), Pessimistic Locking (`SELECT FOR UPDATE`), Autonomous Transactions.
4. **Storage**: Table Partitioning (Range/Hash/List), Temporary Tables (Global vs Session), Time Travel/Flashback, Change Data Capture (CDC), Object-Relational Collections.
5. **Identity**: Sequences, Identity Columns / Auto-increment, UUID Generation.

### 4.2 Programmatic Citation Validation & Matrix Construction
- You must create a feature matrix constructed with the following exact column headers to account for Spanner's dual-dialect nature:
  - `Category`
  - `Feature Supported in Source`
  - `Spanner GoogleSQL Equivalent Feature` (If supported in Spanner GoogleSQL natively or via workaround, output the explicit Spanner feature name. If not, output 'Unsupported')
  - `Notes (Spanner GoogleSQL)` (Dedicated architectural space to document limitations, workarounds, or application-tier refactoring requirements specific to Spanner GoogleSQL)
  - `Spanner PostgreSQL Equivalent Feature` (If supported in Spanner PostgreSQL natively or via workaround, output the explicit Spanner feature name. If not, output 'Unsupported')
  - `Notes (Spanner PostgreSQL)` (Dedicated architectural space to document limitations, workarounds, or application-tier refactoring requirements specific to Spanner PostgreSQL)
  - `Source DB Documentation URL` (Direct citation proving the source database's behavior)
  - `Spanner Documentation URL` (Direct citation proving Spanner's side of the support claim across dialects)
- For every feature evaluated, you MUST populate both URL columns using search tools to prevent relying on internal LLM memory.

### 4.3 Open-Ended Proprietary Discovery
- After completing the Master List, you must proactively execute searches for the source database's *"Architecture Overview"* or *"Release Notes"*.
- You must discover and append **at least 3 wildly unique or proprietary features** specific to this source dialect that were not mentioned in the Master List, ensuring we do not miss undocumented edge cases.

## 5. Dual-Format Artifact Generation & User Approval
1. **Store the Draft Baseline Artifacts (Dual Formats)**:
   - **Datatype Matrix**: Create the markdown version (`<source_dialect>_datatype_mapping_matrix.md`) and automatically export it as a strict CSV (`<source_dialect>_datatype_mapping_matrix.csv`).
   - **Feature Matrix**: Create the markdown version (`<source_dialect>_feature_mapping_matrix.md`) and automatically export it as a strict CSV (`<source_dialect>_feature_mapping_matrix.csv`).
   - *Rule*: Rigorously wrap CSV fields inside double-quotes (`""`) to prevent delimiter breakage from commas or newlines.
2. **User Confirmation Block**:
   - Present the Markdown mapping tables visually to the user.
   - Inform the user that the corresponding `.md` and `.csv` formats have been saved to the workspace.
   - Stop and ask the user to confirm/approve the mappings or provide custom overrides.
3. **Apply Overrides & Verify**:
   - If the user suggests any changes, update **both** the `.md` and `.csv` files immediately to reflect the feedback.
   - Once the user explicitly approves the artifacts, confirm they are ready to be passed downstream.
