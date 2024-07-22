---
layout: default
title: Custom Transformation
parent: Minimal downtime migrations
nav_order: 4
---

# Minimal downtime migrations for MySQL
{: .no_toc }

For cases where a user wants to handle a custom transformation logic, they need to specify the following parameters in the [Datastream To Spanner](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/datastream-to-spanner) template - a GCS path that points to a custom jar, fully classified custom class name of the class containing custom transformation logic and custom parameters which might be used by the jar to invoke custom logic to perform transformation.

Steps to perfrom customization:
1. Implement custom transformation logic for forward migration in the [toSpannerRow](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-custom-shard/src/main/java/com/custom/CustomTransformationFetcher.java#L42) method of the **CustomTransformationFetcher.java**. Details of the MigrationTransformationRequest class can be found [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-migrations-sdk/src/main/java/com/google/cloud/teleport/v2/spanner/utils/MigrationTransformationRequest.java).
2. Build the [JAR](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/spanner-custom-shard) and upload the jar to GCS
3. Invoke the datastream-to-spanner template by passing the custom jar path and custom class path.
4. If any custom parameters are needed in the custom transformation logic, they can be passed via the *customParameters* input to the template. These parameters will be passed to the *init* method of the custom class. The *init* method is invoked once per worker setup.

Implementation details for custom transformation:
1. [MigrationTransformationRequest](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-migrations-sdk/src/main/java/com/google/cloud/teleport/v2/spanner/utils/MigrationTransformationRequest.java) contains the following information - 
    - tableName - Name of the source table to which the event belongs to.
    - shardId - Logical shard id of the record.
    - eventType - The event type can either be INSERT, UPDATE-INSERT, UPDATE, UPDATE_DELETE or DELETE. Please refer to the [datastream documentation](https://cloud.google.com/datastream/docs/events-and-streams) for more details.
    - requestRow - It is a map where key is the source column name and value is source column value.
2. [MigrationTransformationResponse](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-migrations-sdk/src/main/java/com/google/cloud/teleport/v2/spanner/utils/MigrationTransformationResponse.java) contains the following information - 
    - responseRow - It is a map where key is the spanner column name and value is spanner column value.
    - isEventFiltered - If set to true, event will be skipped and not written to spanner.
3. Values in the response row should be of same datatype as the spanner schema.
4. Please throw **InvalidTransformationException** in case of any error while processing a particular event in custom jar.

Here is a table that details the source data type for **MySQL**, its corresponding request row object type, spanner datatype and the expected response format:
| Source datatype         | Request object type                 | Spanner datatype      | Response format                                                                        |
|-------------------------|-------------------------------------|-----------------------|----------------------------------------------------------------------------------------|
| TINYINT                 | Long                              | INT64     | Long                                                                                 |
| INT                     | Long                              | INT64     | Long                                                                                 |
| BIGINT                  | Long                              | INT64    | Long                                                                                 |
| TIME                    | Long ([time-micros](https://avro.apache.org/docs/current/specification/_print/#time-microsecond-precision) ex: 45296000000 for 12:34:56) | STRING      | Long|
| YEAR                    | Long                              | STRING     | Long                                                                                 |
| FLOAT                   | Double                          | FLOAT32     | Double                                                                                 |
| DOUBLE                  | Double                          | FLOAT64     | Double                                                                                 |
| DECIMAL                 | String                              | NUMERIC      | String                                                                                 |
| BOOLEAN                 | Long                | BOOLEAN     | Long                                                                                 |
| TEXT                    | String                              | STRING     | String                         |
| ENUM                    | String                              | STRING     | String                               |
| BLOB                    | String (hex encoded)             | BYTES     | Binary String                                                                          |
| BINARY                  | String (hex encoded)             | BYTES     | Binary String                                                                          |
| BIT                     | Long             | BYTES     | Long                                                                          |
| DATE                    | String (Format: yyyy-MM-dd))        | DATE     | String( Format: yyyy-MM-dd)            |
| DATETIME                | String (ex: 2024-01-01T12:34:56Z)   | TIMESTAMP     | String                                                                                 |
| TIMESTAMP               | String (ex: 2024-01-01T12:34:56Z)   | TIMESTAMP     | String                                                                                 |


Please refer to the sample implementation of **toSpannerRow** for all MySQL datatype columns [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-custom-shard/src/main/java/com/custom/CustomTransformationWithShardForIT.java#L44).