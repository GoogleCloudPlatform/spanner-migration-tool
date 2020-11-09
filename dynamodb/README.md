# HarbourBridge: Turnkey DynamoDB-to-Spanner Evaluation

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing PostgreSQL or MySQL or DynamoDB database. This
README provides details of the tool's DynamoDB capabilities. For general
HarbourBridge information see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-turnkey-spanner-evaluation).

## Example DynamoDB Usage

The following examples assume `harbourbridge` has been added to your PATH
environment variable.

HarbourBridge can be run directly on a DynamoDB database (via the aws go sdk
package).

Before running HarbourBridge, make sure that you have
[set up your AWS credentials/region](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html)
correctly.

To use the tool directly on a DynamoDB database (it will migrate all tables),
run

```sh
harbourbridge -driver=dynamodb
```

Due the schemaless nature of DynamoDB, the tool infers the schema based on a
certain amount of sampled data, by default, 100000 rows. If a table has more
rows than the default value, we only use 100000 rows for estimating the schema.
You can change this value, run

```sh
harbourbridge -driver=dynamodb -schema-sample-size=1000000
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

DynamoDB is schemaless and it is impossible to get the full information of the
schema. So we sample a set of rows and use them to model the schema in Cloud
Spanner. For a large-scale table, scanning the entire table would be extremely
slow. Therefore, we can only scan a part of it. We provide a flag
`schema-sample-size` to specify how many rows to use for schema inference.

In addition, for the performance purpose, we choose to call Scan on the table
instead of randomly sample rows. This might be biased but we believe that the 
result of Scan is not order-guaranteed and it can represent a sampled part
of the table.

### `Number`

The Number type in DynamoDB is encoded as a string, which can represent an
integer or float value. The range of the
[Numeric type](https://cloud.google.com/spanner/docs/storing-numeric-data)
in Cloud Spanner is smaller than the range of the
[Number type](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html)
in DynamoDB. The conversion would result in out of range with potential
precision loss. For simplicity, we will first try to convert it into a NUMERIC
type. If it fails, we convert it into a STRING type. 

### `Null` Data Type

In DynamoDB, an attribute can have a Null data type that represents an unknown
or undefined state. Also, each row defines its own schema for attributes (not
for primary keys). So attributes can be absent in rows. 

We treat the above two cases the same as a Null value in Cloud Spanner. The
cases that a column contains a Null value or a column is not present is an
indication that this column should be nullable.

### `List` and `Map`

In Cloud Spanner, the most similar type to List and Map is
[STRUCT](https://cloud.google.com/spanner/docs/data-types#struct_type), but it
is not a valid column type (available for query but not for storage).
Therefore, we encode them into a json string. 

## Data Conversion

### A Scan for Entire Table

A pass for the entire table is required. We will use Scan API to read data. Each
read has a size limit up to 1MB. By using the returned token, we make a
subsequent call to continue retrieving data from the table.

The row result contains the data type and data itself. According to our
[inferred schema](#schema-inference), we will parse the row to a format that
Cloud Spanner can support. If the value parsing fails, we would drop the entire
row and record it as bad data in the report. If a column does not appear or 
column has a NULL data type, we would process this as a NULL value in Cloud Spanner. 
The job is done when all rows are inserted into the table in Cloud Spanner. 
