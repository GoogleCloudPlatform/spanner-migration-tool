# HarbourBridge: CSV-to-Spanner Migration

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation 
and migration. We now support loading data from CSVs. This assumes a Spanner
database with Schema already exists and HarbourBridge loads the data for you.


To run HarbourBridge to migrate CSVs, it should be used only with the data 
subcommand. It also requires a manifest file as an input which has Table names,
Column names and Data types of your Spanner schema.

### Manifest File
The manifest file should be a list of JSONs containing each table's
information. Each item should contain the fields:
- `"table_name"`: The name of the table, identical to the corresponding table 
name in your Spanner schema.
- `"file_patterns"`: A list of the CSV file paths that contain data for that
table. Note that it is a list, so multiple CSV files are accepted per table.
- `"columns"`: A list having the column data types identical to the Spanner
table's schema. It should specify the column_name and data types for each 
column in the table. Both column names and type names should be identical
to what is there in your Spanner schema. 

**Sample manifest:**
```
[
    {
      "table_name": "Albums",
      "file_patterns": [
        "/Users/username/Desktop/harbourbridge/Albums.csv"
      ],
      "columns": [
        {"column_name": "a", "type_name": "BOOL"},
        {"column_name": "b", "type_name": "BYTES(124)"},
        {"column_name": "c", "type_name": "DATE"},
        {"column_name": "d", "type_name": "FLOAT64"},
        {"column_name": "e", "type_name": "INT64"},
        {"column_name": "f", "type_name": "NUMERIC"},
        {"column_name": "g", "type_name": "STRING"},
        {"column_name": "h", "type_name": "TIMESTAMP"},
        {"column_name": "i", "type_name": "JSON"}
      ]
    },
    {
      "table_name": "Singers",
      "file_patterns": [
        "/Users/username/Desktop/harbourbridge/Singers_1.csv"
        "/Users/username/Desktop/harbourbridge/Singers_2.csv"
        "/Users/username/Desktop/harbourbridge/Singers_3.csv"
      ],
      "columns": [
        {"column_name": "SingerId", "type_name": "INT64"},
        {"column_name": "FirstName", "type_name": "STRING(100)"},
        {"column_name": "LastName", "type_name": "STRING"}
      ]
    }
]
```
**CAVEATS:**
- File patterns do not accept regular expressions. Provide the path inside 
double quotes.
- The type_name should be identical to the types in spanner schema. Only 
for `STRING` and `BYTES`, the length can be optionally omitted.

### CSV File Format
- Each column in a row should be separated by a `,`(comma).
- The first row in each file should contain the column names, which should
be same as the corresponding Spanner column names.
- Remove trailing spaces, tabs in the column name headers.

**Sample CSV:**
```
bool_col,byte_col,date_col,float_col,int_col,numeric_col,string_col,timestamp_col,json_col
true,bytevalue,2020-12-09,15.13,100,39.94,Helloworld,2019-10-29 05:30:00,"{""key1"": ""value1"", ""key2"": ""value2""}"
```
**CSV Data Type Considerations:**
- We only support scalar data types right now. Sample data format for each type
provided in the snippet above.
- The only supported data format right now is **RFC3339 full-date format**.
- The only supported timestamp format right now is **ISO 8601**.
- The format to escape the quotes in json is adding an additional `"` in front
of the double quote. `\` does not work. Also enclose the whole data inside "".
Some modification might be required since most databases do not export CSVs with escaping quotes like mentioned.


## Example CSV Usage

The following examples assume a `harbourbridge` alias has been setup as 
described in the [Installing HarbourBridge](https://github.com/cloudspannerecosystem/harbourbridge#installing-harbourbridge) section of the main README.


To run harbourbridge for loading CSVs, run the following command

```sh
harbourbridge data -source=csv -source-profile="manifest=manifest_file.json" -target-profile="instance=my-instance,dbname=my-db" 
```

You can also specify the project in the target profile optionally. If not
provided, HarbourBridge will search for the project ID in the gcloud 
config, followed by the environment variable `GCLOUD_PROJECT`. Project, Instance and DBName
fields in the target profile are mandatory to identify which Spanner database
to write to.

## Foreign Keys and Interleaved Tables

HarbourBridge currently does not guarantee data conversion if the Spanner
schema has Foreign Keys or Interleaved Tables in the final schema.
It is recommended to create Foreign Keys only after data conversion
has been completed.

For interleaved tables, it may not be possible to change the schema later.
The recommended workaround for this is to reorder the tables in the list
inside the manifest file such that the parent tables are described before
the child tables.
