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
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
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
			inputString:       "secret_key=v1, secret_key=v2",
			expectedParams:    map[string]string{"secret_key": "v1"},
			errorExpected:     true,
			notContainedInErr: "secret_key",
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
			expectedOutput:         "host=0.0.0.0 port=3306 user=user password=password dbname=database sslmode=disable",
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
			expectedOutputPg:    "host=0.0.0.0 port=3306 user=user password=password dbname=db sslmode=disable",
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

func TestSanitizeConnectionError(t *testing.T) {
	secret := "SuperSecretPass_9988"

	assert.NoError(t, SanitizeConnectionError(nil))

	// 1. Direct *url.Error (e.g. Oracle / SQL Server invalid port)
	_, urlErr := url.Parse("oracle://admin:" + secret + "@localhost:bad_port/xe")
	assert.Error(t, urlErr)
	sanitized := SanitizeConnectionError(fmt.Errorf("open failed: %w", urlErr))
	assert.NotContains(t, sanitized.Error(), secret)
	assert.Contains(t, sanitized.Error(), "invalid port")

	// 2. Nested *url.Error
	nestedErr := fmt.Errorf("connection wrapper: %w", &url.Error{
		Op:  "parse",
		URL: "postgres://user:" + secret + "@localhost:5432/db",
		Err: errors.New("underlying failure"),
	})
	sanitizedNested := SanitizeConnectionError(nestedErr)
	assert.NotContains(t, sanitizedNested.Error(), secret)
	assert.Contains(t, sanitizedNested.Error(), "<redacted>")

	// 3. Plain network error preserved as-is
	plainErr := errors.New("dial tcp 127.0.0.1:5432: connect: connection refused")
	assert.Equal(t, plainErr, SanitizeConnectionError(plainErr))
}

