# HarbourBridge: CSV-to-Spanner Migration

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation 
and migration. We now support loading data from CSVs. This assumes a Spanner
database with schema already exists and HarbourBridge loads the data for you.
It first reads the schema in the database specified by your target profile
to understand how to convert the data to relevant types. If using PG Spanner,
you should specify the dialect in the target-profile explicitly.
**For CSVs, you need to run harbourbridge in data mode only**

## Example CSV Usage

You can load CSVs to Spanner in 2 ways.

- **Without manifest input:**
 Before running, make sure each table's csv is present in the current working 
directory named `[table_name].csv` where table_name is the same as the Spanner
table name. 
Harbourbridge will migrate `table_name.csv` to a Spanner table named
`table_name` in the database specified via the target profile.

```sh
spanner-migration-tool data -source=csv -target-profile="instance=my-instance,dbName=my-db,dialect=postgresql" 
```

- **Providing a manifest input:**
 You can optionally provide a manifest file if you have csv files in different
locations (local system as well as Google Cloud Storage). You can also provide 
multiple csv file paths for a single table using the manifest.

```sh
spanner-migration-tool data -source=csv -source-profile="manifest=path/to/manifest/file" -target-profile="instance=my-instance,dbName=my-db" 
```

### Manifest File
The manifest file should be a list of JSONs containing each table's
csv file locations. Each item should contain the fields:
- `"table_name"`: The name of the table, identical to the corresponding table 
name in your Spanner schema.
- `"file_patterns"`: A list of the CSV file paths (local and GCS) that contain 
data for that table. Note that it is a list, so multiple CSV files are accepted 
per table.

**Sample manifest:**
```
[
    {
      "table_name": "Albums",
      "file_patterns": [
        "/Users/username/Desktop/Albums.csv"
      ]
    },
    {
      "table_name": "Singers",
      "file_patterns": [
        "/Users/username/Desktop/tmp/Singers_1.csv",
        "gs://bucket-name/Singers_2.csv",
        "/Users/username/Downloads/other/Singers_3.csv"
      ]
    }
]
```
**CAVEATS:**
- File patterns do not accept regular expressions. Provide the path inside 
double quotes.

### CSV File Format
- Harbourbridge checks the first row and matches it with the spanner columns
to check if the first row is a permutation of the Spanner table's column names.
If a match is not found, then Harbourbridge assumes the order of data in csv
is same as the corresponding Spanner table's column.
- You can optionally provide a custom ordering by providing the column names in
the first row.
- Remove trailing spaces, tabs between delimiters and data.
- Array data has to be enclosed within `[]` or `{}`.
- Default delimiter is `,` but can be specified via the delimiter flag in source
profile.
- Null values are represented by `''` by default. This can be specified via the 
`nullStr` flag in source profile.

For example, if you want to use `|` as the delimiter and `NULL` as the null value, 
you can use 
```sh
spanner-migration-tool data -source=csv -source-profile="delimiter=|,nullStr=NULL" -target-profile="instance=my-instance,dbName=my-db" 
```


**Sample CSV:**
You can just provide the data in the file
```
true,helloworld,10
```
or provide col names in a custom order
```
int_col,bool_col,string_col
10,true,helloworld
```

Example data types:
```
bool_col,byte_col,date_col,float_col,int_col,numeric_col,string_col,timestamp_col,json_col
true,bytevalue,2020-12-09,15.13,100,39.94,Helloworld,2019-10-29 05:30:00,"{""key1"": ""value1"", ""key2"": ""value2""}"
```
Example array data:
```
string_col,int_col,array_col
abc,10,"[1,2,3]"
xyz,11,"{1,2,3}"
```
Since the delimiter is also `,`, the array has to be enclosed within `""`
to make it unambiguous. You can choose a different delimiter for the columns
to avoid this (like `|`). The array data however should only be separated by `,`.
An alternate approach would be using it like `abc|10|[1,2,3]`.
As for enclosing the array data with `[]` or `{}`, you can use either.

**CSV Data Type Considerations:**

- The only supported date format right now is **RFC3339 full-date format**.
- The only supported timestamp format right now is **ISO 8601**.
- The format to escape the quotes in json is adding an additional `"` in front
of the double quote. `\` does not work. Also enclose the whole data inside "".
Some modification might be required since most databases do not export CSVs with escaping quotes like mentioned.
