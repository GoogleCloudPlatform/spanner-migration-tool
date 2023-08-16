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
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/performance"
)

const (
	dbName = "testdb"
)

// Performs the cleanup - drops the MYSQL database and deletes the csv file created for loading data into MYSQL database
func main() {
	host, user, password, port := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLPWD"), os.Getenv("MYSQLPORT")
	connString := performance.GetMYSQLConnectionStr(host, port, user, password, "")
	db, err := sql.Open("mysql", connString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Drop MySQL database.
	_, err = db.Exec("DROP DATABASE IF EXISTS " + dbName)
	if err != nil {
		panic(err)
	}

	// Delete the csv file.
	if _, err := os.Stat("records.csv"); err == nil {
		err = os.Remove("records.csv")
		if err != nil {
			log.Fatalln("failed to delete file", err)
		}
	}
}
