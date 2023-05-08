## Harbourbridge Report
A Harbourbridge report consists of elements defined below. A sample structured report can be found [here](/test_data/mysql_structured_report.json).

#### Summary
Defines the overall quality of the conversion perfomed by Harbourbridge along with a rating.

#### Migration Type
Defines the type of conversion performed by Harbourbridge. It is one of SCHEMA, DATA and SCHEMA_AND_DATA.

#### Ignored Statements
Defines the statements in the source schema which have been ignored by Harbourbridge. For example, View related statements are currently ignored by Harbourbridge.
#### Conversion Metdata
Defines the total time taken to perform the conversion. This may include other conversion related metadata in the future.

#### Statement Stats
Statistics on different types of statements identified by Harbourbridge. This is only populated when processing dump files.

#### Name Changes
Renaming related changes done by Harbourbridge to ensure Cloud Spanner compatibility.

#### Individual Table Reports
Detailed table-by-table analysis showing how many columns were converted perfectly, with warnings etc.

#### Unexpected Conditions
Unexpected conditions encountered by Harbourbridge while processing the source schema/data.

