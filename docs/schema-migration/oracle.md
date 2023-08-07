---
layout: default
title: Oracle
parent: Schema migration
nav_order: 4
---

# Schema migration for Oracle
{: .no_toc }

Spanner migration tool makes some assumptions while performing data type conversion from Oracle to Spanner.
There are also nuances to handling certain specific data types. These are captured below.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## USER and SCHEMA handling

In Oracle DB, USER is the account name, SCHEMA is the set of objects owned by that user. Oracle creates the SCHEMA object as part of the CREATE USER statement and the SCHEMA has the same name as the USER.

`dbName` will be the SID of the Database used. The Oracle System ID (SID) is used to uniquely identify a particular database on a system.

## Data type mapping

| Oracle Type        | Spanner Type |
| ---------------------- | ------------ |
| NUMBER (* , 0)         | INT64        |
| FLOAT                  | FLOAT64      |
| BINARY_FLOAT           | FLOAT64      |
| BINARY_DOUBLE          | FLOAT64      |
| NUMBER (* , >0)        | NUMERIC      |
| CHAR                   | STRING(1)    |
| NCHAR                  | STRING(N)    |
| VARCHAR                | STRING(MAX)  |
| VARCHAR2               | STRING(MAX)  |
| NVARCHAR2              | STRING(MAX)  |
| CLOB                   | STRING(MAX)  |
| NCLOB                  | STRING(MAX)  |
| LONG                   | STRING(MAX)  |
| ROWID                  | STRING(MAX)  |
| UROWID                 | STRING(MAX)  |
| DATE                   | DATE         |
| TIMESTAMP              | TIMESTAMP    |
| BLOB                   | BYTES        |
| BFILE                  | BYTES        |
| RAW                    | BYTES        |
| LONG RAW               | BYTES        |
| XMLTYPE                | STRING(MAX)  |
| INTERVAL YEAR          | STRING(MAX)  |
| INTERVAL DAY           | STRING(MAX)  |
| GEOMETRY               | STRING(MAX)  |
| JSON                   | JSON         |
