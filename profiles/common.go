// Copyright 2020 Google LLC
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

package profiles

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	go_ora "github.com/sijms/go-ora/v2"
)

// Parses input string `s` as a map of key-value pairs. It's expected that the
// input string is of the form "key1=value1,key2=value2,..." etc. Return error
// otherwise.
func ParseMap(s string) (map[string]string, error) {
	params := make(map[string]string)
	if len(s) == 0 {
		return params, nil
	}

	// We use CSV reader to parse key=value pairs separated by a comma to
	// handle the case where a value may contain a comma within a quote. We
	// expect exactly one record to be returned.
	r := csv.NewReader(strings.NewReader(s))
	r.Comma = ','
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return params, err
	}
	if len(records) > 1 {
		return params, fmt.Errorf("contains invalid newline characters")
	}

	for _, kv := range records[0] {
		s := strings.Split(strings.TrimSpace(kv), "=")
		if len(s) != 2 {
			return params, fmt.Errorf("invalid key=value pair (expected format: key1=value1): %v", kv)
		}
		if _, ok := params[s[0]]; ok {
			return params, fmt.Errorf("duplicate key found: %v", s[0])
		}
		params[s[0]] = s[1]
	}
	return params, nil
}

func ParseList(s string)([]string, error) {
	if (len(s) == 0) {
		return nil, nil
	}
	r := csv.NewReader(strings.NewReader(s))
	r.Comma = ','
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) > 1 {
		return nil, fmt.Errorf("contains invalid newline characters")
	}
	return records[0], nil
}

func GetSQLConnectionStr(sourceProfile SourceProfile) string {
	sqlConnectionStr := ""
	if sourceProfile.Ty == SourceProfileTypeConnection {
		switch sourceProfile.Conn.Ty {
		case SourceProfileConnectionTypeMySQL:
			connParams := sourceProfile.Conn.Mysql
			return getMYSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypePostgreSQL:
			connParams := sourceProfile.Conn.Pg
			return getPGSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypeDynamoDB:
			// For DynamoDB, client provided by aws-sdk reads connection credentials from env variables only.
			// Thus, there is no need to create sqlConnectionStr for the same. We instead set the env variables
			// programmatically if not set.
			return ""
		case SourceProfileConnectionTypeSqlServer:
			connParams := sourceProfile.Conn.SqlServer
			return getSQLSERVERConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypeOracle:
			connParams := sourceProfile.Conn.Oracle
			return getORACLEConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		}
	}
	return sqlConnectionStr
}

func GeneratePGSQLConnectionStr() (string, error) {
	server := os.Getenv("PGHOST")
	port := os.Getenv("PGPORT")
	user := os.Getenv("PGUSER")
	dbName := os.Getenv("PGDATABASE")
	if server == "" || port == "" || user == "" || dbName == "" {
		fmt.Printf("Please specify host, port, user and database using PGHOST, PGPORT, PGUSER and PGDATABASE environment variables\n")
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = utils.GetPassword()
	}
	return getPGSQLConnectionStr(server, port, user, password, dbName), nil
}

func getPGSQLConnectionStr(server, port, user, password, dbName string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", server, port, user, password, dbName)
}

func GenerateMYSQLConnectionStr() (string, error) {
	server := os.Getenv("MYSQLHOST")
	port := os.Getenv("MYSQLPORT")
	user := os.Getenv("MYSQLUSER")
	dbName := os.Getenv("MYSQLDATABASE")
	if server == "" || port == "" || user == "" || dbName == "" {
		fmt.Printf("Please specify host, port, user and database using MYSQLHOST, MYSQLPORT, MYSQLUSER and MYSQLDATABASE environment variables\n")
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("MYSQLPWD")
	if password == "" {
		password = utils.GetPassword()
	}
	return getMYSQLConnectionStr(server, port, user, password, dbName), nil
}

func getMYSQLConnectionStr(server, port, user, password, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, server, port, dbName)
}

func getSQLSERVERConnectionStr(server, port, user, password, dbName string) string {
	return fmt.Sprintf(`sqlserver://%s:%s@%s:%s?database=%s`, user, password, server, port, dbName)
}

func GetSchemaSampleSize(sourceProfile SourceProfile) int64 {
	schemaSampleSize := int64(100000)
	if sourceProfile.Ty == SourceProfileTypeConnection {
		if sourceProfile.Conn.Ty == SourceProfileConnectionTypeDynamoDB {
			if sourceProfile.Conn.Dydb.SchemaSampleSize != 0 {
				schemaSampleSize = sourceProfile.Conn.Dydb.SchemaSampleSize
			}
		}
	}
	return schemaSampleSize
}

func getORACLEConnectionStr(server, port, user, password, dbName string) string {
	portNumber, _ := strconv.Atoi(port)
	return go_ora.BuildUrl(server, portNumber, dbName, user, password, nil)
}
