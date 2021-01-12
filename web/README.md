# HarbourBridge: Web APIs (Experimental)

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing PostgreSQL or MySQL database. This README provides
details of the tool's Web APIs capabilities, Web APIs allows user to visualize the conversion
using UI and interact with the tool to customize some of the conversion. For general HarbourBridge information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-turnkey-spanner-evaluation). Note that this feature is experimental, and should not be used for production database.

### Starting web server for HarbourBridge

The following example assume `harbourbridge` has been added to your PATH
environment variable.

HarbourBridge's Web API feature can be used with all the driver modes available, using mysql or postgres dump or direct connection.

To start HarbourBridge web server, run:

```sh
harbourbridge --web
```

The tool will be available on port 8080

You can go to `<base-url>/` to visit the UI of the tool.

    eg: localhost:8080/

For more details on how to use the UI, you can visit: `<base-url>/userManual.html`.

    eg: localhost:8080/userManual.html

<ins>**Note:**</ins>

Data migration is not supported when using `pg_dump` or `mysqldump` driver options when the converted schema contains Interleaved tables. It is recommended to use `postgres` or `mysql` driver for data migration with schema having Interleaved tables.

## APIs

These are the REST APIs and their details:

### Connect

`/connect` is a POST API which is used in case of direct connection. It takes driver config in request body, following is the json structure for the same.

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

(1) `/convert/infoschema` is a GET API followed by `/connect` API to convert using infoschema mode. It returns the schema conversion state in json format.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Conv struct in JSON format.

(2) `/convert/dump` is a POST API which is used to convert schema from dump file. It takes dump config in request body , following is the json structure for the same.

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

`/ddl` is a GET API which must be used after using conversion APIs (i.e, `/connect` or `/convert`). This API returns the DDL statements of the converted schema.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Table wise DDL statements.

Example

```json
{
  "Albums": "--\n-- Spanner schema for source table Albums\n--\nCREATE TABLE Albums (\n    SingerId INT64 NOT NULL, -- From: SingerId smallint(6)\n    AlbumId INT64 NOT NULL,  -- From: AlbumId smallint(6)\n    AlbumTitle STRING(50)    -- From: AlbumTitle varchar(50)\n) PRIMARY KEY (SingerId, AlbumId)"
}
```

### Session

(1) `/session` is a GET API which returns the schema conversion state in json format. It also create a file with suffix `.session.json` in the `frontend/` folder.

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
  "CreatedAt": "2021-01-01T00:00:00.000000+00:00"
}
```

(2) `/session/resume` is a POST API which can be used to resume the previous session, it requires the session file which can be generated using the above API. It takes following as request body.

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

`/summary` is a GET API which returns table wise summary of the conversion.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Table wise summary of conversion.

Example

```json
{
  "Albums": "Note\n1) Some columns will consume more storage in Spanner e.g. for column 'AlbumId', source DB type smallint(6) is mapped to Spanner type int64.\n\n"
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

`/conversion` is a GET API which returns table wise rate of conversion which is encoded in color values.

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
  "Userinfo": "RED"
}
```

### Typemap

(1) `/typemap` is a GET API which returns the source type to list of spanner type map, which can be used for manual type maping from the UI.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Map from source type to list of possible spanner type and issue.

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

(2) `/typemap/global` is a POST API which converts from source type map to desired spanner typemap globally (i.e, it affects all the tables). It takes a map from source type to spanner type in json format.

#### Method

`POST`

#### Request body

Map from source type to spanner type.

Example

```json
{
  "smallint": "STRING",
  "varchar": "BYTES"
}
```

#### Response body

Updated Conv struct in JSON format.

(3) `/typemap/table?table=<table_name>` is a POST API which performs following operations on a single table.

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
- ToType : New spanner type or empty string

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

### Filepaths

`/filepaths` is a GET API which generates and returns file paths for schema and report file.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

Paths to schema and report files.

Example

```json
{
  "Report": "/path/to/reportfile.report.txt",
  "Schema": "/path/to/schemafile.schema.txt"
}
```

### Interleave tables

`/checkinterleave/table?table=<table_name>` is a GET API which checks it it is possible to convert this table into interleaved table in spanner, if it is possible it converts and returns parent table name, otherwise it returns failure message.

#### Method

`GET`

#### Request body

No request body is needed.

#### Response body

If it is possible to make given table interleaved, it will return with table name of parent.

Example

```json
{
  "Possible": true,
  "Parent": "Singers",
  "Comment": ""
}
```

If it is not possible to interleave given table, it will return with the reason of why it failed.

Example

```json
{
  "Possible": false,
  "Parent": "",
  "Comment": "No valid prefix"
}
```
