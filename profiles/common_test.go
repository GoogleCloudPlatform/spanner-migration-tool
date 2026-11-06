// Copyright 2024 Google LLC
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
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// code for testing parse map
func TestParseMap(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name              string
		inputString       string
		expectedParams    map[string]string
		errorExpected     bool
		notContainedInErr string
	}{
		{
			name:           "empty params",
			inputString:    "",
			expectedParams: map[string]string{},
			errorExpected:  false,
		},
		{
			name:           "valid params",
			inputString:    "instance=instance",
			expectedParams: map[string]string{"instance": "instance"},
			errorExpected:  false,
		},
		{
			name:           "valid params with = in value",
			inputString:    "password=pass=word",
			expectedParams: map[string]string{"password": "pass=word"},
			errorExpected:  false,
		},
		{
			name:           "valid params with special characters in value",
			inputString:    "\"password=password`~!@#$%^&*()-_=+[]{}|;:<,>.?/'\\\"",
			expectedParams: map[string]string{"password": "password`~!@#$%^&*()-_=+[]{}|;:<,>.?/'\\"},
			errorExpected:  false,
		},
		{
			name:              "invalid params incorrect format does not leak value",
			inputString:       "uuwy",
			expectedParams:    map[string]string{},
			errorExpected:     true,
			notContainedInErr: "uuwy",
		},
		{
			name:              "invalid params unquoted comma in password does not leak secret",
			inputString:       "user=admin,password=part1,secret_part2",
			expectedParams:    map[string]string{"user": "admin", "password": "part1"},
			errorExpected:     true,
			notContainedInErr: "secret_part2",
		},
		{
			name:              "invalid params empty key does not leak value",
			inputString:       "=secret_value",
			expectedParams:    map[string]string{},
			errorExpected:     true,
			notContainedInErr: "secret_value",
		},
		{
			name:           "invalid params new line char",
			inputString:    "uuwy\n hjgse",
			expectedParams: map[string]string{},
			errorExpected:  true,
		},
		{
			name:              "invalid params duplicates does not leak key",
			inputString:       "instance=instance, instance=instance",
			expectedParams:    map[string]string{"instance": "instance"},
			errorExpected:     true,
			notContainedInErr: "instance",
		},
		{
			name:              "invalid params unquoted comma with duplicate = in password does not leak secret",
			inputString:       "user=admin,password=part1,secretChunk=1,secretChunk=2",
			expectedParams:    map[string]string{"user": "admin", "password": "part1", "secretChunk": "1"},
			errorExpected:     true,
			notContainedInErr: "secretChunk",
		},
	}

	for _, tc := range testCases {
		res, err := ParseMap(tc.inputString)
		assert.Equal(t, tc.expectedParams, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		if err != nil && tc.notContainedInErr != "" {
			assert.NotContains(t, err.Error(), tc.notContainedInErr, tc.name)
		}
	}
}

func TestFormatProfileParamsRoundTrip(t *testing.T) {
	passwords := []string{
		"simplePass",
		`p"ass"word`,
		`my"sec,ret=1\2'3`,
		`x",dup=1,dup=2,"y`,
		"multi\nline\tpass",
	}
	for _, pwd := range passwords {
		encoded, err := FormatProfileParams(
			"host=127.0.0.1",
			"port=5432",
			`user=ad"min,1`,
			"password="+pwd,
			`dbName=my"db,1`,
		)
		require.NoError(t, err)
		parsed, err := ParseMap(encoded)
		require.NoError(t, err)
		assert.Equal(t, "127.0.0.1", parsed["host"])
		assert.Equal(t, "5432", parsed["port"])
		assert.Equal(t, `ad"min,1`, parsed["user"])
		assert.Equal(t, pwd, parsed["password"])
		assert.Equal(t, `my"db,1`, parsed["dbName"])
	}
}

// code for testing parse list
func TestParseList(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name           string
		inputString    string
		expectedParams []string
		errorExpected  bool
	}{
		{
			name:           "empty input string",
			inputString:    "",
			expectedParams: nil,
			errorExpected:  false,
		},
		{
			name:           "valid input string",
			inputString:    "hello, world",
			expectedParams: []string{"hello", "world"},
			errorExpected:  false,
		},
		{
			name:           "invalid input string new line char",
			inputString:    "hello, world\n, !",
			expectedParams: nil,
			errorExpected:  true,
		},
	}

	for _, tc := range testCases {
		res, err := ParseList(tc.inputString)
		assert.Equal(t, tc.expectedParams, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
	}
}

// code for testing sql connection string
func TestGetSQLConnectionStr(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	host := "0.0.0.0"
	port := "3306"
	user := "user"
	pwd := "password"
	db := "database"
	testCases := []struct {
		name                   string
		inputSourceProfileConn SourceProfileConnection
		expectedOutput         string
	}{
		{
			name:                   "source profile connection type mysql",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeMySQL, Mysql: SourceProfileConnectionMySQL{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "user:password@tcp(0.0.0.0:3306)/database",
		},
		{
			name:                   "source profile connection type postgres",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypePostgreSQL, Pg: SourceProfileConnectionPostgreSQL{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "host='0.0.0.0' port='3306' user='user' password='password' dbname='database' sslmode=disable",
		},
		{
			name: "source profile connection type postgres with special characters and spaces",
			inputSourceProfileConn: SourceProfileConnection{
				Ty: SourceProfileConnectionTypePostgreSQL,
				Pg: SourceProfileConnectionPostgreSQL{
					Host: "localhost",
					Port: "5432",
					User: "user'#1",
					Pwd:  "passw\\'`~ord #1",
					Db:   "test 'db\\#1",
				},
			},
			expectedOutput: "host='localhost' port='5432' user='user\\'#1' password='passw\\\\\\'`~ord #1' dbname='test \\'db\\\\#1' sslmode=disable",
		},
		{
			name:                   "source profile connection type sql server",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeSqlServer, SqlServer: SourceProfileConnectionSqlServer{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "sqlserver://user:password@0.0.0.0:3306?database=database",
		},
		{
			name: "source profile connection type sql server with special characters and spaces",
			inputSourceProfileConn: SourceProfileConnection{
				Ty: SourceProfileConnectionTypeSqlServer,
				SqlServer: SourceProfileConnectionSqlServer{
					Host: "localhost",
					Port: "1433",
					User: "user#1",
					Pwd:  "passw\\`~ord#1",
					Db:   "test db#1",
				},
			},
			expectedOutput: "sqlserver://user%231:passw%5C%60~ord%231@localhost:1433?database=test+db%231",
		},
		{
			name:                   "source profile connection type sql server ipv6",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeSqlServer, SqlServer: SourceProfileConnectionSqlServer{Host: "::1", Port: "1433", User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "sqlserver://user:password@[::1]:1433?database=database",
		},
		{
			name:                   "source profile connection type oracle",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeOracle, Oracle: SourceProfileConnectionOracle{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "oracle://user:password@0.0.0.0:3306/database",
		},
	}

	for _, tc := range testCases {
		res := GetSQLConnectionStr(SourceProfile{Ty: SourceProfileType(SourceProfileTypeConnection), Conn: tc.inputSourceProfileConn})
		assert.Equal(t, tc.expectedOutput, res, tc.name)
	}
}

func TestGenerateConnectionStr(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	before := func() {
		setEnvVariables()
	}

	after := func() {
		unsetEnvVariables()
	}
	testCases := []struct {
		name                string
		expectedOutputPg    string
		expectedOutputMysql string
		errorExpected       bool
	}{
		{
			name:                "valid get mysql and postgres conn string",
			expectedOutputPg:    "host='0.0.0.0' port='3306' user='user' password='password' dbname='db' sslmode=disable",
			expectedOutputMysql: "user:password@tcp(0.0.0.0:3306)/db",
			errorExpected:       false,
		},
	}

	for _, tc := range testCases {
		before()
		res, err := GeneratePGSQLConnectionStr()
		assert.Equal(t, tc.expectedOutputPg, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		res, err = GenerateMYSQLConnectionStr()
		assert.Equal(t, tc.expectedOutputMysql, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		after()
	}
}

func TestGetSchemaSampleSize(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name               string
		inputSourceProfile SourceProfile
		expectedOutput     int64
	}{
		{
			name:               "mysql source profile type",
			inputSourceProfile: SourceProfile{Ty: SourceProfileType(SourceProfileTypeConnection), Conn: SourceProfileConnection{Ty: SourceProfileConnectionTypeMySQL}},
			expectedOutput:     int64(100000),
		},
	}

	for _, tc := range testCases {
		res := GetSchemaSampleSize(tc.inputSourceProfile)
		assert.Equal(t, tc.expectedOutput, res, tc.name)
	}
}

func TestGetORACLEConnectionStrSpecialCharacters(t *testing.T) {
	testCases := []struct {
		name     string
		user     string
		password string
	}{
		{"plain", "SMT_USER", "Secret123"},
		{"password with space", "SMT_USER", "Smt case 18"},
		{"password with plus", "SMT_USER", "a+b"},
		{"password with reserved characters", "SMT_USER", "p@ss:w/rd?#%&="},
		{"user with # and $", "C##APP$1", "pw"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			connStr := GetORACLEConnectionStr("127.0.0.1", "1521", tc.user, tc.password, "XEPDB1")
			// go-ora parses the connection string with url.Parse.
			u, err := url.Parse(connStr)
			assert.NoError(t, err)
			assert.Equal(t, "oracle", u.Scheme)
			assert.Equal(t, tc.user, u.User.Username())
			pwd, _ := u.User.Password()
			assert.Equal(t, tc.password, pwd)
			assert.Equal(t, "127.0.0.1:1521", u.Host)
			assert.Equal(t, "/XEPDB1", u.Path)
		})
	}
}

type recordingPQDialer struct {
	network string
	address string
}

func (d *recordingPQDialer) Dial(network, address string) (net.Conn, error) {
	d.network = network
	d.address = address
	return nil, errors.New("dial intercepted")
}

func (d *recordingPQDialer) DialTimeout(network, address string, _ time.Duration) (net.Conn, error) {
	return d.Dial(network, address)
}

func TestGetPGSQLConnectionStrDriverRoundTrip(t *testing.T) {
	testCases := []struct {
		name              string
		host              string
		port              string
		user              string
		password          string
		dbName            string
		expectedNet       string
		expectedAddr      string
		checkPGXRoundTrip bool
	}{
		{
			name:              "complex credentials with spaces quotes backslashes newlines and slashes",
			host:              "127.0.0.1",
			port:              "5432",
			user:              "user '1\\2\" #%",
			password:          "myPass 's3cretTail\\end\" \t\n/db?sslmode=require\\",
			dbName:            "my/db 'name\\1",
			expectedNet:       "tcp",
			expectedAddr:      "127.0.0.1:5432",
			checkPGXRoundTrip: true,
		},
		{
			name:              "ipv6 host",
			host:              "::1",
			port:              "5433",
			user:              "postgres",
			password:          "s3cret",
			dbName:            "mydb",
			expectedNet:       "tcp",
			expectedAddr:      "[::1]:5433",
			checkPGXRoundTrip: true,
		},
		{
			name:              "cloudsql unix domain socket path",
			host:              "/cloudsql/my-project:us-central1:my-instance",
			port:              "5432",
			user:              "postgres",
			password:          "s3cret 'quote\\",
			dbName:            "/leading/slash/db",
			expectedNet:       "unix",
			expectedAddr:      "/cloudsql/my-project:us-central1:my-instance/.s.PGSQL.5432",
			checkPGXRoundTrip: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := GetPGSQLConnectionStr(tc.host, tc.port, tc.user, tc.password, tc.dbName)

			_, err := pq.NewConnector(dsn)
			require.NoError(t, err)

			dialer := &recordingPQDialer{}
			_, err = pq.DialOpen(dialer, dsn)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), tc.password)
			assert.Equal(t, tc.expectedNet, dialer.network)
			assert.Equal(t, tc.expectedAddr, dialer.address)

			if tc.checkPGXRoundTrip {
				cfg, err := pgx.ParseConfig(dsn)
				require.NoError(t, err)
				assert.Equal(t, tc.host, cfg.Host)
				assert.Equal(t, tc.user, cfg.User)
				assert.Equal(t, tc.password, cfg.Password)
				assert.Equal(t, tc.dbName, cfg.Database)
			}
		})
	}

	t.Run("invalid port or host never leaks password", func(t *testing.T) {
		secret := "SuperSecretPgPass!987"
		dsn := GetPGSQLConnectionStr("bad host", "5432a", "postgres", secret, "mydb")
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			assert.NotContains(t, err.Error(), secret)
			return
		}
		defer db.Close()
		err = db.Ping()
		require.Error(t, err)
		assert.NotContains(t, err.Error(), secret)
	})
}

func TestGetMYSQLConnectionStrDriverRoundTrip(t *testing.T) {
	dsn := GetMYSQLConnectionStr("::1", "3306", "user", "p@ss:w/rd?1#2'\"\\ ", "my/db?name")
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "[::1]:3306", cfg.Addr)
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "p@ss:w/rd?1#2'\"\\ ", cfg.Passwd)
	assert.Equal(t, "my/db?name", cfg.DBName)
}

func TestSanitizeConnectionError(t *testing.T) {
	secret := "MyTopSecretCredential!42"

	t.Run("sqlserver invalid port error is sanitized", func(t *testing.T) {
		dsn := GetSQLSERVERConnectionStr("localhost", "1433a", "sa", secret, "mydb")
		db, err := sql.Open("sqlserver", dsn)
		if err == nil {
			defer db.Close()
			err = db.Ping()
		}
		require.Error(t, err)
		sanitized := SanitizeConnectionError(err)
		assert.NotContains(t, sanitized.Error(), secret)
	})

	t.Run("oracle invalid port error is sanitized", func(t *testing.T) {
		dsn := GetORACLEConnectionStr("localhost", "1521a", "system", secret, "XE")
		db, err := sql.Open("oracle", dsn)
		if err == nil {
			defer db.Close()
			err = db.Ping()
		}
		require.Error(t, err)
		sanitized := SanitizeConnectionError(err)
		assert.NotContains(t, sanitized.Error(), secret)
	})

	t.Run("wrapped url.Error and flattened credential URLs are redacted", func(t *testing.T) {
		_, rawURLErr := url.Parse("postgres://admin:" + secret + "@localhost:5432a/mydb")
		require.Error(t, rawURLErr)
		wrapped := fmt.Errorf("failed to connect: %w", rawURLErr)
		assert.NotContains(t, SanitizeConnectionError(wrapped).Error(), secret)

		flattened := errors.New("driver failed on oracle://admin:" + secret + "@localhost:1521a/XE")
		assert.NotContains(t, SanitizeConnectionError(flattened).Error(), secret)
	})
}
