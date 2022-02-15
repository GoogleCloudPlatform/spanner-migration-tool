package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/performance"
)

const (
	dbName = "testdb"
)

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
