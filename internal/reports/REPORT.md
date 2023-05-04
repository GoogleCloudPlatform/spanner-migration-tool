## Harbourbridge Report
A Harbourbridge report consists of the following elements:

#### Summary
Defines the overall quality of the conversion perfomed by Harbourbridge.

#### Ignored statements
Defines the statements in the source schema which have been ignored by Harbourbridge. For example, View related statements are currently ignored by Harbourbridge.
#### Conversion duration
Total time taken to perform the conversion.

#### Statement stats
Statistics on different types of statements identified by Harbourbridge. This is only populated when processing dump files.

#### Name changes
Renaming related changes done by Harbourbridge to ensure Cloud Spanner compatibility.

#### Individual table reports
Detailed table-by-table analysis showing how many columns were converted perfectly, with warnings etc.

#### Unexpected conditions
Unexpected conditions encountered by Harbourbridge while processing the source schema/data.

