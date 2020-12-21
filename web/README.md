<!-- Work in progress -->

# HarbourBridge: Web APIs

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation,
using data from an existing PostgreSQL or MySQL database. This README provides
details of the tool's Web APIs capabilities. For general HarbourBridge information
see this [README](https://github.com/cloudspannerecosystem/harbourbridge#harbourbridge-turnkey-spanner-evaluation).

### Starting web server for HarbourBridge

The following example assume `harbourbridge` has been added to your PATH
environment variable.

HarbourBridge's Web API feature can be used with all the driver modes available, using mysql or postgres dump or direct connection.

To start HarbourBridge web server,run:

```sh
harbourbridge --web
```

You can go to `localhost:8080/frontend/` to visit the UI of the tool.

## APIs

These are the REST APIs and their details:

### Connect

`/connect` is a POST API which is used in case of direct connection. It takes drive config in request body, following is the json structure for the same.

```
{
	Driver   string
	Host     string
	Port     string
	Database string
	User     string
	Password string
}
```

### Convert

`/convert/infoschema` is a GET API followed by `/connect` API to convert using infoschema mode. It returns the schema conversion state in json format.

`/convert/dump` is a POST API which is used to convert schema from dump file. It takes dump config in request body , following is the json structure for the same.

```
{
	Driver   string
	FilePath string
}
```

This API returns the schema conversion state in json format.

### DDL

`/ddl` is a GET API which must be used after using conversion APIs (i.e, `/connect` or `/convert`). This API returns the DDL statements of the converted schema.

### Session

`/session` is a GET API which returns the schema conversion state in json format. It also create a file with suffix `.session.json` in the `frontend/` folder.

`/session/resume` is a POST API which can be used to resume the previous session, it requires the session file which can be generated using the above API. It takes following as request body.

```
{
	Driver    string
	FilePath  string
	FileName  string
	CreatedAt time.Time
}
```

### Summary

`/summary` is a GET API which returns table wise summary of the conversion.

### Overview

`/overview` is a GET API which returns the overview of the conversion.

### Conversion

`/conversion` is a GET API which returns table wise rate of conversion which is encoded in color values.

### Typemap

`/typemap` is a GET API which returns the source type to list of spanner type map, which can be used for manual type maping from the UI.

`/typemap/global` is a POST API which converts from source type map to desired spanner typemap globally (i.e, it affects all the tables). It takes a map from source type to spanner type in json format.

`/typemap/table?table=<table_name>` is a POST API which performs following operations on a single table.

- Remove column
- Rename column
- Remove or Add primary key
- Update type of column
- Remove or Add NOT NULL constraint

This API takes following as request body:

```
{
    UpdateCols
    {
                "column_name"
                {
                    Removed: true|false
                    Rename: "<new name>"
                    PK: ""|"ADDED"|"REMOVED"
                    NotNull: ""|"ADDED"|"REMOVED"
                    ToType: "<spanner type>"
                }
    }
}
```

### Filepaths

`/filepaths` is a GET API which generates and returns file paths for schema and report file.

### Interleave tables

`/checkinterleave/table?table=<table_name>` is a GET API which checks it it is possible to convert this table into interleaved table in spanner, if it is possible it converts and returns parent table name, otherwise it returns failure message.
