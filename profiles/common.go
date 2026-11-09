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
	"bytes"
	"encoding/csv"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	mysqldriver "github.com/go-sql-driver/mysql"
)

var libpqEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

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

// FormatProfileParams encodes key=value entries as a single CSV record
// compatible with ParseMap, properly quoting values with commas, quotes, or newlines.
func FormatProfileParams(pairs ...string) (string, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(pairs); err != nil {
		return "", err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
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
			return GetMYSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypePostgreSQL:
			connParams := sourceProfile.Conn.Pg
			return GetPGSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
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
	return GetPGSQLConnectionStr(server, port, user, password, dbName), nil
}

func quoteLibpqValue(v string) string {
	return "'" + libpqEscaper.Replace(v) + "'"
}

// GetPGSQLConnectionStr builds a libpq keyword/value connection string with
// single-quoted, backslash-escaped values so spaces, quotes, backslashes, IPv6
// hosts, and Unix socket paths round-trip without net/url.Parse leaks.
func GetPGSQLConnectionStr(server, port, user, password, dbName string) string {
	var parts []string
	if server != "" {
		parts = append(parts, "host="+quoteLibpqValue(server))
	}
	if port != "" {
		parts = append(parts, "port="+quoteLibpqValue(port))
	}
	if user != "" {
		parts = append(parts, "user="+quoteLibpqValue(user))
	}
	if password != "" {
		parts = append(parts, "password="+quoteLibpqValue(password))
	}
	if dbName != "" {
		parts = append(parts, "dbname="+quoteLibpqValue(dbName))
	}
	parts = append(parts, "sslmode=disable")
	return strings.Join(parts, " ")
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
	return GetMYSQLConnectionStr(server, port, user, password, dbName), nil
}

// GetMYSQLConnectionStr builds a MySQL DSN using the official Go MySQL driver Config
// to safely handle passwords with special characters (e.g. '@', '/', spaces, quotes).
func GetMYSQLConnectionStr(server, port, user, password, dbName string) string {
	cfg := mysqldriver.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%s", server, port)
	cfg.DBName = dbName
	return cfg.FormatDSN()
}

func getSQLSERVERConnectionStr(server, port, user, password, dbName string) string {
	u := url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%s", server, port),
	}
	q := u.Query()
	q.Set("database", dbName)
	u.RawQuery = q.Encode()
	return u.String()
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
