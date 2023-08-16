// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/performance"
	"github.com/go-sql-driver/mysql"
)

const (
	dbName = "testdb"
)

var (
	recordCount     int
	multipleTableDb bool
)

// Populates both single table database and multiple table database based on the flag
// multiple-table-db and the number of records to be inserted to the database is passed
// via record-count. Schema for the database to be created is static.
func main() {
	flag.IntVar(&recordCount, "record-count", 10000, "record-count: Number of rows to add")
	flag.BoolVar(&multipleTableDb, "multiple-table-db", false, "multiple-table-db: it is set to true for populating multiple table database")
	flag.Parse()
	file, err := os.Create("records.csv")
	defer file.Close()
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	w := csv.NewWriter(file)
	defer w.Flush()

	host, user, password, port := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLPWD"), os.Getenv("MYSQLPORT")
	connString := performance.GetMYSQLConnectionStr(host, port, user, password, "")
	db, err := sql.Open("mysql", connString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("USE " + dbName)
	if err != nil {
		panic(err)
	}
	if !multipleTableDb {
		// Writing data to csv file.
		var data [][]string
		for i := 1; i <= recordCount; i++ {
			row := []string{performance.RandomString(5), performance.RandomString(10), performance.RandomString(10), performance.RandomString(50), performance.RandomDate(),
				strconv.FormatBool(performance.RandomBool()), strconv.FormatFloat(performance.RandomFloat(150, 200), 'E', -1, 64), strconv.Itoa(int(performance.RandomInt(1000, 100000))), performance.CurrentTimestamp()}
			data = append(data, row)
		}
		w.WriteAll(data)

		// MySQL table creation.
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS employee(employee_id varchar(50) PRIMARY KEY, first_name varchar(50) NOT NULL, 
		last_name varchar(50), address varchar(100), dob DATE NOT NULL, is_manager bool NOT NULL, height_in_cm float(4,1) NOT NULL, 
		salary integer NOT NULL, last_updated_time TIMESTAMP NOT NULL)`)
		if err != nil {
			panic(err)
		}
		connString = performance.GetMYSQLConnectionStr(host, port, user, password, "testdb")
		if err != nil {
			panic(err)
		}
		db, err = sql.Open("mysql", connString)
		if err != nil {
			panic(err)
		}

		// Loading data into MySQL database from the locally generated csv file.
		mysql.RegisterLocalFile("records.csv")
		_, err = db.Exec("LOAD DATA LOCAL INFILE 'records.csv' INTO TABLE employee FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'; ")
		if err != nil {
			panic(err.Error())
		}
	}
}
