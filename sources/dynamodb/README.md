# Spanner migration tool: DynamoDB-to-Spanner Evaluation and Migration

Spanner migration tool (formerly known as HarbourBridge) is a stand-alone open source tool for Cloud Spanner evaluation and migration,
using data from an existing database. This
README provides details of the tool's DynamoDB capabilities. For general
Spanner migration tool information see this [README](https://github.com/GoogleCloudPlatform/spanner-migration-tool#spanner-migration-tool-spanner-evaluation-and-migration).

## Example DynamoDB Usage

Before running Spanner migration tool, make sure that you have
[set up your AWS credentials/region](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html)
correctly (set the environment variables `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_REGION`). Spanner migration tool accesses your
DynamoDB database via the aws go sdk package. If you use a custom endpoint
for dynamodb, you can specify that using the environment variable
`DYNAMODB_ENDPOINT_OVERRIDE`.

The following examples assume a harbourbridge alias has been setup as described
in the [Installing Spanner migration tool](https://github.com/GoogleCloudPlatform/spanner-migration-tool#installing-spanner-migration-tool) section of the main README.

For example, run

```sh
export DYNAMODB_ENDPOINT_OVERRIDE=http://dynamodb.us-west-2.amazonaws.com
harbourbridge schema -source=dynamodb 
```

Instead of setting the environment variables, you
can also pass corresponding source profile connection parameters `aws-access-key-id`
, `aws-secret-access-key`, `aws-region`. Custom endpoint can be specified using
`dydb-endpoint` param.

For example, to perform schema conversion, run

```sh
harbourbridge schema -source=dynamodb -source-profile="aws-access-key-id=<>,aws-secret-access-key=<>,aws-region=<>"
```

This will generate a session file with `session.json` suffix. This file contains
schema mapping from source to destination. You will need to specify this file
during data migration. You also need to specify a particular Spanner instance and database to use
during data migration.

For example, run

```sh
harbourbridge data -session=mydb.session.json -source=dynamodb -source-profile="aws-access-key-id=<>,..." -target-profile="instance=my-spanner-instance,,dbName=my-spanner-database-name"
```

You can also run Spanner migration tool in a schema-and-data mode, where it will perform both
schema and data migration. This is useful for quick evaluation when source
database size is small.

```sh
harbourbridge schema-and-data -source=dynamodb -source-profile="aws-access-key-id=<>,..." -target-profile="instance=my-spanner-instance,..."
```

Spanner migration tool generates a report file, a schema file, and a bad-data file (if
there are bad-data rows). You can control where these files are written by
specifying a file prefix. For example,

```sh
harbourbridge schema -prefix=mydb. -source=dynamodb -source-profile="aws-access-key-id=<>,..."
```

will write files `mydb.report.txt`, `mydb.schema.txt`, and
`mydb.dropped.txt`. The prefix can also be a directory. For example,

```sh
harbourbridge schema -prefix=~/spanner-eval-mydb/ -source=dynamodb -source-profile="aws-access-key-id=<>,..."
```

would write the files into the directory `~/spanner-eval-mydb/`. Note
that Spanner migration tool will not create directories as it writes these files.

Spanner migration tool accepts an additional param `schema-sample-size` for
`-source-profile` for DynamoDB. Due to the schemaless nature of DynamoDB, the
tool infers the schema based on a certain amount of sampled data, by default,
100000 rows. If a table has more rows than the default value, we only use
100000 rows for estimating the schema. This flag lets you specify the number
of rows to use for inferring schema. The default value is 100,000.

Sample usage:

```sh
harbourbridge schema -source=dynamodb -source-profile="schema-sample-size=500000,aws-access-key-id=<>,..."
```

## DynamoDB Streaming Migration Usage

- DynamoDB Streams will be used for Change Data Capture in streaming migration.
- If there exists any DynamoDB Stream for a given table, then it must be of StreamViewType
`NEW_IMAGE` or `NEW_AND_OLD_IMAGES`. If this condition is not followed then this table will
not be considered for streaming migration.

### Steps

1. Start the streaming migration. Example usage
```sh
harbourbridge schema-and-data -source=dynamodb -source-profile="aws-access-key-id=<>,...,enableStreaming=<>" -target-profile="instance=my-spanner-instance,..."
```
Valid choices for enableStreaming: `yes`, `no`, `true`, `false`

**Regular Updates**: Count of records processed and if the current moment is optimum for switching to Cloud Spanner or not will be updated regularly at an interval of 1 minute.

2. If you want to switch to Cloud Spanner then stop the writes on the source DynamoDB database and press Ctrl+C. After that remaining unprocessed records within DynamoDB Streams will be processed. Wait for it to get finished.

3. Switch to Cloud Spanner once the whole migration process is completed.

## Schema Conversion

The Spanner migration tool maps DynamoDB types to Spanner types as follows:

| DynamoDB Type      | Spanner Type               | Notes                                     |
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
with a fairly well defined set of columns and types. Our Spanner migration tool support
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
column has a NULL data type, we would process this as a NULL value in
Cloud Spanner.
