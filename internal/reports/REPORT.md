## Spanner migration Report
A Spanner migration report consists of elements defined below. A sample structured report can be found [here](/test_data/mysql_structured_report.json).

#### Summary
Defines the overall quality of the conversion perfomed by the Spanner migration tool along with a rating.

#### Migration Type
Defines the type of conversion performed by the Spanner migration tool. It is one of SCHEMA, DATA and SCHEMA_AND_DATA.

#### Ignored Statements
Defines the statements in the source schema which have been ignored by the Spanner migration tool. For example, View related statements are currently ignored by the Spanner migration tool.
#### Conversion Metdata
Defines the total time taken to perform the conversion. This may include other conversion related metadata in the future.

#### Statement Stats
Statistics on different types of statements identified by the Spanner migration tool. This is only populated when processing dump files.

#### Name Changes
Renaming related changes done by the Spanner migration tool to ensure Cloud Spanner compatibility.

#### Individual Table Reports
Detailed table-by-table analysis showing how many columns were converted perfectly, with warnings etc.

#### Unexpected Conditions
Unexpected conditions encountered by the Spanner migration tool while processing the source schema/data.

