#!/bin/sh
if type mysql >/dev/null 2>&1; then
    echo "MySQL present."
    export MYSQLHOST=localhost
    export MYSQLUSER=root
    export MYSQLPORT=3306
    #update MySQL password before running the benchmark
    export MYSQLPWD=
    gcloud auth application-default login
    #benchmark for single table database
    for insertRecordCount in 250000 250000 1000000
    do
        go run ./performance/populate_database/populate_database.go -record-count $insertRecordCount
        echo "$insertRecordCount records inserted."
        for nodes in 1 3 5
        do
            gcloud spanner instances create new-test-instance --config=regional-us-central1 \
            --description="New test Instance" --nodes=$nodes
            gcloud config set spanner/instance new-test-instance
            echo "Spanner instance created with $nodes nodes"
            for writeLimit in 20 40 60 80 100
            do
                echo "Write limit: $writeLimit"
                #update source profile password before running the benchmark
                go run main.go schema-and-data -source=mysql -source-profile='host=localhost,user=root,db_name=testdb,password=' -target-profile='instance=new-test-instance,dbname=testdb' -write-limit $writeLimit
            done
            yes | gcloud spanner instances delete test-instance
        done
    done
    go run ./performance/cleanup_resource/cleanup_resource.go
    #benchmark for multiple table database
    for insertRecordCount in 250000 250000 1000000
    do
        go run ./performance/populate_database/populate_database.go -record-count $insertRecordCount -multiple-table-db
        echo "$insertRecordCount records inserted."
        for nodes in 1 3 5
        do
            gcloud spanner instances create new-test-instance --config=regional-us-central1 \
            --description="New test Instance" --nodes=$nodes
            gcloud config set spanner/instance new-test-instance
            for threads in 20 40 60 80 100
            do
                echo "Write limit: $writeLimit"
                #update source profile password before running the benchmark
                go run main.go schema-and-data -source=mysql -source-profile='host=localhost,user=root,db_name=testdb,password=' -target-profile='instance=new-test-instance,dbname=testdb' -write-limit $writeLimit
            done
            yes | gcloud spanner instances delete test-instance
        done
    done
    go run ./performance/cleanup_resource/cleanup_resource.go
else
    echo "MySQL not present. Please install it from: https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-install.html"
fi