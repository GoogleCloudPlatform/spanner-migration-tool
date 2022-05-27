# HarbourBridge: Performance benchmarking

HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation 
and migration. This module can be used to run benchmark tests on single table
and multiple table database migration using HarbourBridge.

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
- **Create spanner instance:**
    ```sh
    gcloud spanner instances create new-test-instance --config=regional-us-central1 --description="New test Instance" --nodes=1
    gcloud config set spanner/instance new-test-instance
    ```
- **Run migration:**
Run migration for the test database created.
We can specify the maximum number of writer threads in write-limit flag.
Enter the password for connecting to MYSQL db and run the below command.
    ```sh
    go run main.go schema-and-data -source=mysql -source-profile='host=localhost,user=root,dbName=testdb,password='
     -target-profile='instance=new-test-instance,dbName=testdb' -write-limit 40
    ```
- **Cleanup:**
Once the migration is complete, the last step is cleanup. It involves deleting the spanner instance and droping the MYSQL database.
  
  For deleting the spanner instance, run:
    ```sh
    gcloud spanner instances delete test-instance
    ```
  For cleaning up MYSQL resources, run:
    ```sh
    go run ./performance/cleanup_resource/cleanup_resource.go
    ```


The benchmarking process runs the above steps for different database sizes, spanner instances and different number of writer threads.
The time taken for schema and data migration respectively are printed in the report generated after each migration.