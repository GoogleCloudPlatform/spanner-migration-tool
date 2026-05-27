---
layout: default
title: Custom Transformations
nav_order: 16
permalink: /custom-transformation
---

# Custom transformation
{: .no_toc }
Users can follow this guide to apply custom transformations at the row level to all columns in their schema, including primary key columns. This guide can be used to specify custom transformation logic for existing columns, to populate new columns in Spanner that didn't exist in the source, or to filter a row during migration. For ease of use, we have implemented the [CustomTransformationFetcher](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-custom-shard/src/main/java/com/custom/CustomTransformationFetcher.java) class in `v2/spanner-custom-shard`, which can be updated according to the user's logic.

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Custom transformation workflow

![](https://services.google.com/fh/files/misc/transformation-workflow.png)

Points to note regarding the workflow:
- The workflow will execute once per event and will also be triggered during retries.
- Not all column values need to be returned; only the returned columns will be updated. The rest will be migrated as they are from the source.
- Users can add new columns in the response that are not present in the source but exist in Spanner. These column values will be written to Spanner, and the data types of the returned values must be compatible with the Spanner schema.

## Methods in CustomTransformationFetcher

- **init()** - This is an initialization method that will be called once during the pipeline setup. It is used to initialize the custom jar with custom parameters.  
- **toSpannerRow()** - This method applies custom transformations to the incoming source record and is expected to return a subset of spanner row.

## Parameter details

### Request
[MigrationTransformationRequest](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-migrations-sdk/src/main/java/com/google/cloud/teleport/v2/spanner/utils/MigrationTransformationRequest.java) contains the following information - 
- tableName - Name of the source table to which the event belongs to.
- shardId - Logical shard id of the record.
- eventType - The event type can either be INSERT, UPDATE-INSERT, UPDATE, UPDATE_DELETE or DELETE.
- requestRow - It is a map of type `Map<java.lang.String, java.lang.Object>` where key is the source column name and value is source column value.

The following table outlines the Java object type that will be sent in the request to the `toSpannerRow` for each **MySQL** datatype:

| MYSQL datatype | Java object type    |
|----------------|---------------------|
| BIGINT         | Long                |
| BINARY         | String (hex encoded)|
| BIT            | Long                |
| BLOB           | String (hex encoded)|
| BOOLEAN        | Long                |
| CHAR           | String              |
| DATE           | String (Format: yyyy-MM-dd) |
| DATETIME       | String (UTC format) |
| DECIMAL        | String              |
| DOUBLE         | Double              |
| ENUM           | String              |
| FLOAT          | Double              |
| INT            | Long                |
| JSON           | String              |
| LONGBLOB       | String (hex encoded)|
| LONGTEXT       | String              |
| MEDIUMBLOB     | String (hex encoded)|
| MEDIUMINT      | Long                |
| MEDIUMTEXT     | String              |
| SET            | String              |
| SMALLINT       | Long                |
| TEXT           | String              |
| TIME           | String (Format: HH-mm-ss) |
| TIMESTAMP      | String (UTC format) |
| TINYBLOB       | String (hex encoded)|
| TINYINT        | Long                |
| TINYTEXT       | String              |
| VARBINARY      | String (hex encoded)|
| VARCHAR        | String              |

{: .highlight }
Values in the request for spatial data type columns will be NULL if not supported by the source connector.

### Response
[MigrationTransformationResponse](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-migrations-sdk/src/main/java/com/google/cloud/teleport/v2/spanner/utils/MigrationTransformationResponse.java) contains the following information - 
- responseRow - It is a map of type `Map<java.lang.String, java.lang.Object>` where key is the spanner column name and value is spanner column value.
- isEventFiltered - If set to true, event will be skipped and not written to spanner.

{: .highlight }
Values in the response row must be compatible with their corresponding Spanner column types.

The following table outlines the recommended Java object types for each Spanner data type(GSQL dialect and PostgreSQL dialect) to ensure successful data insertion:

| Spanner datatype (GSQL dialect) | Spanner datatype (PostgreSQL dialect) | Java object type                                |
|---------------------------------|---------------------------------------|-------------------------------------------------|
| INT64                           | bigint                                | Long                                            |
| FLOAT32                         | real                                  | Double                                          |
| FLOAT64                         | double precision                      | Double                                          |
| NUMERIC                         | numeric                               | String                                          |
| BOOL                            | boolean                               | Boolean                                         |
| STRING                          | text/character varying                | String                                          |
| BYTES                           | bytea                                 | Hex Encoded string                              |
| DATE                            | date                                  | String (Format: yyyy-MM-dd)                     |
| TIMESTAMP                       | timestamp with time zone              | String (in UTC format, e.g., 2023-05-23T12:34:56Z) |
| JSON                            | jsonb                                 | String                                          |

{: .highlight }
Please refer to the sample implementation of **toSpannerRow** for most MySQL datatype columns [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-custom-shard/src/main/java/com/custom/CustomTransformationWithShardForLiveIT.java#L48).

## Steps to implement custom transformation
1. Checkout the dataflow code from [github](https://github.com/GoogleCloudPlatform/DataflowTemplates)
    ```
    git clone https://github.com/GoogleCloudPlatform/DataflowTemplates.git
    ```
2. Update the logic in [CustomTransformationFetcher](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-custom-shard/src/main/java/com/custom/CustomTransformationFetcher.java) class in `v2/spanner-custom-shard`.
3. If any custom parameters are needed in the custom transformation logic, they can be passed to the *init* method of the custom class. The *init* method is invoked once per worker setup.
4. Please test the modified code by writing unit and cross functional tests in [CustomTransformationFetcherTest.java](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-custom-shard/src/test/java/com/custom/CustomTransformationFetcherTest.java) 
5. Build the `spanner-custom-shard` module by running the below commands:
    ```
    cd v2/spanner-custom-shard
    mvn install
    ```
6. Upload the built JAR located in v2/spanner-custom-shard/target with the name `spanner-custom-shard-1.0-SNAPSHOT.jar` to a GCS bucket

## Error handling

- If the value of a column, received as a response from the custom JAR, cannot be successfully parsed into the corresponding Spanner column datatype, pipeline will deem the record as failed and label it as a SEVERE error in the DLQ.
- If the custom JAR sends a NULL response for a NOT NULL column, the record will encounter a failure during insertion and will be labelled as a SEVERE error in the DLQ.
- If the custom JAR returns an exception while processing a record then pipeline will deem the record as failed, label it as a SEVERE error in the DLQ and will increment the `Custom Transformation Exceptions` metric.
<!---
[TODO]: Once the fix to ignore extra columns in response during forward migration is released, update the documentation.
-->
- If the custom JAR returns an extra column in response which is not present in the spanner schema then the record will be labelled as SEVERE error and moved to DLQ.


## Best practices
- Avoid time-consuming operations in the custom JAR, as they can slow down the pipeline since it is executed at a per-record level.
- Ensure idempotency and account for retries.
- Be cautious with logging in the **toSpannerRow** method, as logging per row can cause throttling.
- Throw an **InvalidTransformationException** if an error occurs while processing a particular event in the custom JAR.
