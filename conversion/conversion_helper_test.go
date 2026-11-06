// Copyright 2026 Google LLC
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

package conversion

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	mysqldriver "github.com/go-sql-driver/mysql"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDbNameFromSQLConnectionStrOracle(t *testing.T) {
	tests := []struct {
		name string
		user string
	}{
		{"plain user", "SMT_USER"},
		{"common user with #", "C##SMT_TEST_USER"},
		{"user with #", "APP#1"},
		{"user with $", "HR$OWNER"},
		{"lower case user", "smt_user"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Same connection string format as profiles.GetSQLConnectionStr for Oracle.
			connStr := go_ora.BuildUrl("127.0.0.1", 1521, "XEPDB1", tc.user, "p@ss:w#rd", nil)
			assert.Equal(t, tc.user, getDbNameFromSQLConnectionStr(constants.ORACLE, connStr))
		})
	}
}

func TestGetDbNameFromSQLConnectionStrSqlServer(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
		expected string
	}{
		{
			name:     "plain connection string",
			connStr:  "sqlserver://user:password@localhost:1433?database=testdb",
			expected: "testdb",
		},
		{
			name:     "connection string with additional parameters",
			connStr:  "sqlserver://user:password@localhost:1433?database=testdb&encrypt=true",
			expected: "testdb",
		},
		{
			name:     "database name with spaces and special characters URL encoded",
			connStr:  "sqlserver://user%231:p%40ss%23word@localhost:1433?database=my+test%23db&encrypt=true",
			expected: "my test#db",
		},
		{
			name:     "database name with percent-encoded space",
			connStr:  "sqlserver://user:password@localhost:1433?database=my%20test%20db",
			expected: "my test db",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, getDbNameFromSQLConnectionStr(constants.SQLSERVER, tc.connStr))
		})
	}
}

func TestGetDbNameFromSQLConnectionStrPostgres(t *testing.T) {
	tests := []struct {
		name     string
		connStr  string
		expected string
	}{
		{
			name:     "url format",
			connStr:  "postgres://user:password@localhost:5432/testdb?sslmode=disable",
			expected: "testdb",
		},
		{
			name:     "malformed url does not panic",
			connStr:  "postgres://user:password@localhost:5432a/testdb",
			expected: "",
		},
		{
			name:     "keyword value format",
			connStr:  "host=localhost port=5432 user=user password=password dbname=testdb sslmode=disable",
			expected: "testdb",
		},
		{
			name:     "single quoted keyword value format with spaces quotes backslashes and embedded dbname",
			connStr:  profiles.GetPGSQLConnectionStr("localhost", "5432", "u 1", "pass 'dbname=fake\\1", "real 'db\\name"),
			expected: "real 'db\\name",
		},
		{
			name:     "short or malformed keyword value does not panic",
			connStr:  "host=localhost",
			expected: "",
		},
		{
			name:     "empty string does not panic",
			connStr:  "",
			expected: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, getDbNameFromSQLConnectionStr(constants.POSTGRES, tc.connStr))
		})
	}
}

func TestGetDbNameFromSQLConnectionStrMySQL(t *testing.T) {
	connStr := profiles.GetMYSQLConnectionStr("localhost", "3306", "user", "p@ss)/tricky", "my/test db")
	assert.Equal(t, "my/test db", getDbNameFromSQLConnectionStr(constants.MYSQL, connStr))
	assert.Equal(t, "", getDbNameFromSQLConnectionStr(constants.MYSQL, "malformed"))
}

func TestNewCloudSQLConfigs(t *testing.T) {
	t.Run("postgres config assigns user database and password directly without DSN interpolation", func(t *testing.T) {
		pg := profiles.SourceProfileConnectionCloudSQLPostgreSQL{
			User: "admin '1\\2",
			Db:   "test db options='-c log_statement=all'\\",
			Pwd:  "myPass '1\\2 s3cretTail",
		}
		cfg, err := newCloudSQLPostgresConnConfig(pg)
		require.NoError(t, err)
		assert.Equal(t, pg.User, cfg.User)
		assert.Equal(t, pg.Db, cfg.Database)
		assert.Equal(t, pg.Pwd, cfg.Password)
		assert.NotContains(t, cfg.RuntimeParams, "options")
	})

	t.Run("postgres IAM auth clears PGPASSWORD inherited from environment", func(t *testing.T) {
		t.Setenv("PGPASSWORD", "stale_env_secret")
		pg := profiles.SourceProfileConnectionCloudSQLPostgreSQL{
			User: "iam-user@example.com",
			Db:   "mydb",
			Pwd:  "",
		}
		cfg, err := newCloudSQLPostgresConnConfig(pg)
		require.NoError(t, err)
		assert.Equal(t, "", cfg.Password)
	})

	t.Run("mysql config builds valid connector with special characters", func(t *testing.T) {
		m := profiles.SourceProfileConnectionCloudSQLMySQL{
			User: "user:1@host",
			Db:   "my/db?foo=bar",
			Pwd:  "p@ss:w/rd?1#2",
		}
		cfg := newCloudSQLMySQLConfig(m)
		assert.Equal(t, m.User, cfg.User)
		assert.Equal(t, m.Db, cfg.DBName)
		assert.Equal(t, m.Pwd, cfg.Passwd)
		assert.True(t, cfg.ParseTime)

		mysqldriver.RegisterDialContext("cloudsqlconn", nil)
		_, err := mysqldriver.NewConnector(cfg)
		require.NoError(t, err)
	})
}
