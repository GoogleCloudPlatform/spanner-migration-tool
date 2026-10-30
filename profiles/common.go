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
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
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
		equalSignIndex := strings.Index(kv, "=")
		if equalSignIndex == -1 {
			return params, fmt.Errorf("invalid key=value pair (expected format: key1=value1): %v", kv)
		}

		key := strings.TrimSpace(kv[:equalSignIndex])
		value := kv[equalSignIndex+1:]

		if len(key) == 0 {
			return params, fmt.Errorf("empty key found in pair: %v", kv)
		}

		if _, ok := params[key]; ok {
			return params, fmt.Errorf("duplicate key found: %v", key)
		}
		params[key] = value
	}
	return params, nil
}

func ParseList(s string) ([]string, error) {
	if len(s) == 0 {
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
		case SourceProfileConnectionTypeSqlServer:
			connParams := sourceProfile.Conn.SqlServer
			return getSQLSERVERConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypeOracle:
			connParams := sourceProfile.Conn.Oracle
			return GetORACLEConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
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
		logger.Log.Info(fmt.Sprintf("Please specify host, port, user and database using PGHOST, PGPORT, PGUSER and PGDATABASE environment variables\n"))
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		getInfo := utils.GetUtilInfoImpl{}
		password = getInfo.GetPassword()
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
		logger.Log.Info(fmt.Sprintf("Please specify host, port, user and database using MYSQLHOST, MYSQLPORT, MYSQLUSER and MYSQLDATABASE environment variables\n"))
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("MYSQLPWD")
	if password == "" {
		getInfo := utils.GetUtilInfoImpl{}
		password = getInfo.GetPassword()
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
	return int64(100000)
}

// GetORACLEConnectionStr builds the go-ora connection URL
// (oracle://user:password@host:port/service). The user and password are escaped as
// URL user info (e.g. a space becomes %20), which go-ora decodes back with url.Parse,
// so any character round trips. go_ora.BuildUrl is not used since it escapes them
// with url.QueryEscape, which turns a space into '+' that url.Parse keeps as '+'.
func GetORACLEConnectionStr(server, port, user, password, dbName string) string {
	u := url.URL{
		Scheme: "oracle",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(server, port),
		Path:   "/" + dbName,
	}
	return u.String()
}
