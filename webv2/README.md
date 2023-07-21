# Spanner migration tool: Schema Assistant

Spanner migration tool is a stand-alone open source tool for Cloud Spanner evaluation and
migration. This README provides details of the Spanner migration tool schema assistant,
which supports schema customization. For general Spanner migration tool information see
this [README](https://github.com/GoogleCloudPlatform/spanner-migration-tool).

### Starting web server for Spanner migration tool

The following examples assume `harbourbridge` alias has been setup as
following.

```sh
git clone https://github.com/GoogleCloudPlatform/spanner-migration-tool
cd spanner-migration-tool
alias harbourbridge="go run github.com/GoogleCloudPlatform/spanner-migration-tool"
```

Spanner migration tool's Web API feature can be used with all the driver modes available,
using mysql or postgres dump or direct connection.

To generate the Spanner migration tool binary, run:

```sh
make build
```

To start Spanner migration tool web server, run:

```sh
./harbourbridge web
```

The UI will launched at http://localhost:8080/.

For more details on how to use the UI, click on the `help` button on the top right corner of the page.

<ins>**Note:**</ins>

The `pg_dump` and `mysqldump` drivers cannot be used for data migration if the
Spanner schema has interleaved tables. Note that the `postgres` or `mysql` drivers
do not have this restriction -- consider using these as an alternative.

## APIs

These are the REST APIs and their details:

### Connect

`/connect` is a POST API used to configure direct connection to a database.
The request body contains connection details and the driver name.

#### Method

`POST`

#### Request body

Connection information along with driver name.

Example

```json
{
  "Driver": "postgres",
  "Host": "localhost",
  "Port": "5432",
  "Database": "dbname",
  "User": "user",
  "Password": "password"
}
```

#### Response body

No response body is returned.

### Convert

(1) `/convert/infoschema` is a GET API followed by `/connect` API to convert using
infoschema mode. It returns the schema conversion state in json format.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Conv struct in JSON format.

(2) `/convert/dump` is a POST API used to perform schema conversion on a dump file.
The request body contains the path to the dump file.

#### Method

`POST`

#### Request body

Provide driver and path to dump file.

Example

```json
{
  "Driver": "postgres",
  "Path": "/path/to/dumpFile"
}
```

#### Response body

Conv struct in JSON format.

### DDL

`/ddl` is a GET API which must be used after using conversion APIs (i.e, `/connect`
or `/convert`). This API returns the DDL statements of the converted schema.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Table wise DDL statements.

Example

```json
{
  "Albums": "--\n-- Spanner schema for source table Albums\n--\nCREATE TABLE
Albums (\n    SingerId INT64 NOT NULL, -- From: SingerId smallint(6)\n
AlbumId INT64 NOT NULL, -- From: AlbumId smallint(6)\n    AlbumTitle STRING(50)
-- From: AlbumTitle varchar(50)\n) PRIMARY KEY (SingerId, AlbumId)"
}
```

### Session

(1) `/session` is a GET API which returns the schema conversion state in json format.
It also create a file with suffix `.session.json` in the `frontend/` folder.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Session file info with driver name.

Example

```json
{
  "Driver": "postgres",
  "FilePath": "/path/to/sessionFile",
  "FileName": "sessionFile.session.json",
  "CreatedAt": "Thu, 04 Feb 2021 18:10:32 IST"
}
```

(2) `/session/resume` is a POST API which can be used to resume a previous session.
The request body contains the path to the previous session. Note that sessions are
created by the `/session` API.

#### Method

`POST`

#### Request body

Provide driver and session file path.

Example

```json
{
  "driver": "postgres",
  "path": "/path/to/sessionFile",
  "fileName": "sessionFile.session.json"
}
```

#### Response body

No response body is returned.

### Summary

`/summary` is a GET API which returns a table-by-table report of the conversion.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Table wise summary of conversion.

Example

```json
{
  "Albums": "Note\n1) Some columns will consume more storage in Spanner
e.g. for column 'AlbumId', source DB type smallint(6) is mapped to Spanner
type int64.\n\n"
}
```

### Overview

`/overview` is a GET API which returns the overview of the conversion.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Overall summary of conversion in string format.

### Conversion

`/conversion` is a GET API which returns table wise rate of conversion which is
encoded in color values.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Table wise rate of conversion encoded in color values.

Example

```json
{
  "Albums": "GREEN",
  "Singers": "BLUE",
  "Userinfo": "ORANGE"
}
```

### Typemap

(1) `/typemap` is a GET API which returns a map that, for each source type,
provides the potential Spanner types that can be used for that source type.
This map can be used for customization of type mapping in the UI.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Map from each source type to the list of possible Spanner types and issues.

Example

```json
{
  "smallint": [
    {
      "T": "INT64",
      "Brief": "Some columns will consume more storage in Spanner"
    },
    {
      "T": "STRING",
      "Brief": "Some columns will consume more storage in Spanner"
    }
  ],
  "varchar": [
    {
      "T": "BYTES",
      "Brief": ""
    },
    {
      "T": "STRING",
      "Brief": ""
    }
  ]
}
```

(2) `/typemap/global` is a POST API to customize schema conversion on global level.
The request body contains a map from source type to Spanner type in json format.

#### Method

`POST`

#### Request body

Map from source type to Spanner type.

Example

```json
{
  "smallint": "STRING",
  "varchar": "BYTES"
}
```

#### Response body

Updated Conv struct in JSON format.

(3) `/typemap/table?table=<table_name>` is a POST API which performs following
operations on a single table.

- Remove column
- Rename column
- Remove or Add primary key
- Update type of column
- Remove or Add NOT NULL constraint

#### Method

`POST`

#### Request body

Column wise actions to be performed with following possible values for each field.

- Removed : true/false
- Rename : New name or empty string
- PK : "" | "ADDED" | "REMOVED"
- NotNull : "" | "ADDED" | "REMOVED"
- ToType : New Spanner type or empty string

Example

```json
{
  "UpdateCols": {
    "AlbumTitle": {
      "Removed": false,
      "Rename": "AlbumName",
      "PK": "",
      "NotNull": "ADDED",
      "ToType": "BYTES"
    }
  }
}
```

#### Response body

Updated Conv struct in JSON format.

### Report file

`/report` is a GET API which generates report file and returns file path.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Path to report file.

Example

```
/path/to/reportfile.report.txt
```

### Schema file

`/schema` is a GET API which generates schema file and returns file path.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Path to schema file.

Example

```
/path/to/schemafile.schema.txt
```

### Interleave tables

`/setparent?table=<table_name>` is a GET API which checks whether it is
possible to convert a table into a Spanner interleaved table. If this conversion is possible,
then the schema is changed and the parent table name is returned.
If the conversion is not possible, a failure message is returned.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

If it is possible to make given table interleaved, it will return with table
name of parent.

Example

```json
{
  "Possible": true,
  "Parent": "Singers",
  "Comment": ""
}
```

If it is not possible to interleave given table, it will return with the reason
of why it failed.

Example

```json
{
  "Possible": false,
  "Parent": "",
  "Comment": "No valid prefix"
}
```

### Drop foreign key

`/drop/fk?table=<table_name>&pos=<position>` is a GET API which takes table name
and array position of foreign key in query params. It drops foreign key at given
position for given table name.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Updated Conv struct in JSON format.

### Drop secondary index

`/drop/secondaryindex?table=<table_name>&pos=<position>` is a GET API which takes
table name and array position of secondary index in query params. It drops secondary
index at given position for given table name.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Updated Conv struct in JSON format.
