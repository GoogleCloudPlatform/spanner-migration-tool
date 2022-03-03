# HarbourBridge: Performance benchmarking

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation 
and migration. This module can be used to run benchmark tests on single table
and multiple table database.

For executing the benchmark test run the following command:
```sh
bash ./performance/test.sh
```

## Steps

The benchmarking script comprises of the following steps:

- **Authenticate to gcloud:**
    ```sh
    gcloud auth application-default login
    ```

- **Populate database:**
 We can populate either single table database or multiple table database based
 on the flag and the flag and specify the number of records to be inserted to 
 the database.

  For example, if we want to insert 1000 records to a single table database then
  run the following command:
    ```sh
    go run ./performance/populate_database/populate_database.go -record-count 1000
    ```
    If we want to insert 1000 records to a multiple table database then run the
     below command:
    ```sh
    go run ./performance/populate_database/populate_database.go -record-count 1000 -multiple-table-db
    ```