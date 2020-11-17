# HarbourBridge: Turnkey DynamoDB-to-Spanner Evaluation

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing PostgreSQL or MySQL or DynamoDB database. This
README provides details of the tool's DynamoDB capabilities. For general
HarbourBridge information see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-turnkey-spanner-evaluation).

## Example DynamoDB Usage

The following examples assume `harbourbridge` has been added to your PATH
environment variable.

Before running HarbourBridge, make sure that you have
[set up your AWS credentials/region](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html)
correctly. HarbourBridge accesses your DynamoDB database via the aws go sdk
package.

To use the tool on a DynamoDB database (it will migrate all tables),
run

```sh
harbourbridge -driver=dynamodb
```

Due the schemaless nature of DynamoDB, the tool infers the schema based on a
certain amount of sampled data, by default, 100000 rows. If a table has more
rows than the default value, we only use 100000 rows for estimating the schema.
You can change this value via the flag `schema-sample-size`. For example,

```sh
harbourbridge -driver=dynamodb -schema-sample-size=500000
```

## Schema Conversion

The HarbourBridge tool maps DynamoDB types to Spanner types as follows:

| PostgreSQL Type    | Spanner Type               | Notes                                     |
| ------------------ | -------------------------- | ----------------------------------------- |
| `Number`           | `NUMERIC` or `STRING`      | defaults to NUMERIC, otherwise, STRING    |
| `String`           | `STRING`                   |                                           |
| `Boolean`          | `BOOL`                     |                                           |
| `Binary`           | `BYTES`                    |                                           |
| `Null`             | A nullable column type     |                                           |
| `List`             | `STRING`                   | json encoding                             |
| `Map`              | `STRING`                   | json encoding                             |
| `StringSet`        | `ARRAY<STRING>`            |                                           |
| `NumberSet`        | `ARRAY<NUMERIC or STRING>` |                                           |
| `BinarySet`        | `ARRAY<BYTES>`             |                                           |

We discuss these, as well as other limits and notes on schema conversion, in the
following sections.

### Schema Inference

DynamoDB is a schemaless database: other than a primary index and optional
secondary index, column names and types are essentially unconstrained
and can vary from one row to the next.

However, many customers use DynamoDB in a consistent, structured way
with a fairly well defined set of columns and types. Our HarbourBridge support
for DynamoDB focuses on this use-case, and we construct a Spanner schema
by inspecting table data.

For small tables, we inspect all rows of the table. For large tables, scanning
the entire table would be extremely expensive and slow, and so we only inspect
the first N rows (defined by the flag `schema-sample-size`) from the table scan.
While DynamoDB doesn't return scan results in order, they might not be a truly
random sample of rows. However, the alternative of randomly sampling rows
would be much more expensive.

Columns with consistent types are assigned Spanner types as detailed below.
Columns without a consistent type are mapped to STRING.

#### `Number`

In most cases, we map the Number type in DynamoDB to Spanner's Numeric type.
However, since the [range of Numeric](https://cloud.google.com/spanner/docs/storing-numeric-data)
in Cloud Spanner is smaller than the [range of Number](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html)
in DynamoDB, this conversion could result in out of range with potential
precision loss. To address this possibility, we try to convert the sample data,
and if it consistently fails, we choose STRING type for the column.

#### `Null` Data Type

In DynamoDB, a column can have a Null data type that represents an unknown or
undefined state. Also, each row defines its own schema for columns (not for
primary keys). So columns can be absent in rows. 

We treat the above two cases the same as a Null value in Cloud Spanner. The
cases that a column contains a Null value or a column is not present is an
indication that this column should be nullable.

#### `List` and `Map`

In Cloud Spanner, the most similar type to List and Map is
[STRUCT](https://cloud.google.com/spanner/docs/data-types#struct_type), but it
is not a valid column type (available for query but not for storage).
Therefore, we encode them into a json string. 

#### Occasional Errors

To prevent a few spurious rows from impacting schema construction, we define an
error threshold: when building a type for a column, if the percentage of rows
with a specific data type is lower than or equal to an extremely low
value (0.1%), then we treat those rows as likely errors. Such rows are ignored
for schema construction: their type is not considered a candidate type for the
column.

#### Multi-type Columns

For a special scenario, we may get a column that has equal distribution of two
data types. E.g., a column has 40% rows in String and 60% rows in Number. If we
choose Number as its type, it may lead to 40% data loss in the data conversion.
In the migration, we define a conflicting threshold on rows (after removing Null
data types and rows that the column is not present). By default, the conflicting
threshold is 5% and if the percentages of two or more data types are greater
than it, we would consider that the column has conflicting data types. As a safe
choice, we define this column as a STRING type in Cloud Spanner. 

## Data Conversion

### A Scan for Entire Table

Data conversion proceeds table by table. For each table, we use the Scan API to
read data. Each read has a size limit up to 1MB. By using the returned token, we
make a subsequent call to continue retrieving data from the table.

The row result contains the data type and data itself. According to our
[inferred schema](#schema-inference), we will parse the row to a format that
Cloud Spanner can support. If the value parsing fails, we would drop the entire
row and record it as bad data in the report. If a column does not appear or 
column has a NULL data type, we would process this as a NULL value in Cloud Spanner. 
