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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"
)

// type OsAccMock struct {
// 	os_accessor.OsAccessor
// 	*mock.Mock
// }

// func (oam *OsAccMock) GetEvnVariable(env string) string {
// 	args := oam.Called(env)
// 	return args.Get(0).(string)
// }

func setEnvVariavles(){
	// My Sql variables
	os.Setenv("MYSQLHOST", "0.0.0.0")
	os.Setenv("MYSQLUSER", "user")
	os.Setenv("MYSQLDATABASE", "db")
	os.Setenv("MYSQLPORT", "3306")
	os.Setenv("MYSQLPWD", "password")

	//PG Variables
	os.Setenv("PGHOST", "0.0.0.0")
	os.Setenv("PGUSER", "user")
	os.Setenv("PGDATABASE", "db")
	os.Setenv("PGPORT", "3306")
	os.Setenv("PGPASSWORD", "password")

	// My Sql Server Connection
	os.Setenv("MSSQL_IP_ADDRESS", "0.0.0.0")
	os.Setenv("MSSQL_SA_USER", "user")
	os.Setenv("MSSQL_DATABASE", "db")
	os.Setenv("MSSQL_TCP_PORT", "3306")
	os.Setenv("MSSQL_SA_PASSWORD", "password")
}

type GetUtilInfoMock struct {
	mock.Mock
}

func (gui *GetUtilInfoMock) GetProject() (string, error) {
	args := gui.Called()
	return args.Get(0).(string), args.Error(1)

}

func (gui *GetUtilInfoMock) GetInstance(ctx context.Context, project string, out *os.File) (string, error) {
	args := gui.Called()
	return args.Get(0).(string), args.Error(1)
}

func (gui *GetUtilInfoMock) GetPassword() string {
	args := gui.Called()
	return args.Get(0).(string)
}

func (gui *GetUtilInfoMock) GetDatabaseName(driver string, now time.Time) (string, error) {
	args := gui.Called()
	return args.Get(0).(string), args.Error(1)
}

func setGetInfoMockValues(g *GetUtilInfoMock){
	g.On("GetDatabaseName", mock.AnythingOfType("string"), mock.AnythingOfType("time.Time")).Return("database-id", nil)
	g.On("GetInstance", mock.AnythingOfType("*context.Context"), mock.AnythingOfType("string"), mock.AnythingOfType("*os.File")).Return("instance-id", nil)
	g.On("GetPassword").Return("password")
}

func TestNewSourceProfileFile(t *testing.T) {
	testCases := []struct {
		name         string
		params       map[string]string
		pipedToStdin bool
		want         SourceProfileFile
	}{
		{
			name:         "no params, file piped",
			params:       map[string]string{},
			pipedToStdin: true,
			want:         SourceProfileFile{Format: "dump"},
		},
		{
			name:         "format param, file piped",
			params:       map[string]string{"Format": "dump"},
			pipedToStdin: true,
			want:         SourceProfileFile{Format: "dump"},
		},
		{
			name:         "format and path param, file piped -- piped file takes precedence",
			params:       map[string]string{"format": "dump", "file": "file1.mysqldump"},
			pipedToStdin: true,
			want:         SourceProfileFile{Format: "dump"},
		},
		{
			name:         "format and path param, no file piped",
			params:       map[string]string{"format": "dump", "file": "file1.mysqldump"},
			pipedToStdin: false,
			want:         SourceProfileFile{Format: "dump", Path: "file1.mysqldump"},
		},
		{
			name:         "only path param, no file piped -- default dump format",
			params:       map[string]string{"file": "file1.mysqldump"},
			pipedToStdin: false,
			want:         SourceProfileFile{Format: "dump", Path: "file1.mysqldump"},
		},
	}

	for _, tc := range testCases {
		// Override filePipedToStdin with the test value.
		filePipedToStdin = func() bool { return tc.pipedToStdin }

		profile := NewSourceProfileFile(tc.params)
		assert.Equal(t, profile, tc.want, tc.name)
	}
}

func TestNewSourceProfileConfigFile(t *testing.T) {
	type validationFn func(SourceProfileConfig)
	testCases := []struct {
		name          string
		source        string
		path          string
		errorExpected bool
		validationFn  validationFn
	}{
		{
			name:          "bulk config for mysql",
			source:        "mysql",
			path:          filepath.Join("..", "test_data", "mysql_shard_bulk.cfg"),
			errorExpected: false,
			validationFn: func(spc SourceProfileConfig) {
				assert.NotNil(t, spc.ShardConfigurationBulk)
				assert.NotNil(t, spc.ShardConfigurationBulk.SchemaSource)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.SchemaSource.DbName)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.SchemaSource.Host)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.SchemaSource.Password)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.SchemaSource.Port)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.SchemaSource.User)
				assert.NotNil(t, spc.ShardConfigurationBulk.DataShards)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.DataShards[0].DataShardId)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.DataShards[0].DbName)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.DataShards[0].User)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.DataShards[0].Password)
				assert.NotEmpty(t, spc.ShardConfigurationBulk.DataShards[0].Port)
			},
		},
		{
			name:          "streaming config for mysql",
			source:        "mysql",
			path:          filepath.Join("..", "test_data", "mysql_shard_streaming.cfg"),
			errorExpected: false,
			validationFn: func(spc SourceProfileConfig) {
				assert.NotNil(t, spc.ShardConfigurationDataflow)
				assert.NotNil(t, spc.ShardConfigurationDataflow.SchemaSource)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.SchemaSource.DbName)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.SchemaSource.Host)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.SchemaSource.Password)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.SchemaSource.Port)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.SchemaSource.User)
				assert.NotNil(t, spc.ShardConfigurationDataflow.DataShards)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.DataShards[0].DataShardId)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.DataShards[0].TmpDir)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.DataShards[0].StreamLocation)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.DataShards[0].DataflowConfig)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.DataShards[0].DstConnectionProfile)
				assert.NotEmpty(t, spc.ShardConfigurationDataflow.DataShards[0].SrcConnectionProfile)
			},
		},
		{
			name:          "config for non-mysql",
			source:        "postgres",
			path:          "",
			errorExpected: true,
			validationFn:  func(spc SourceProfileConfig) {},
		},
	}
	for _, tc := range testCases {
		sourceProfileConfig, err := NewSourceProfileConfig(tc.source, tc.path)
		assert.Equal(t, tc.errorExpected, err != nil)
		tc.validationFn(sourceProfileConfig)
	}
}

func TestNewSourceProfileConnectionSQL(t *testing.T) {
	// Avoid getting/settinng env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "mandatory params provided",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "partial mandatory params provided",
			params:        map[string]string{"user": "b", "dbName": "c"},
			errorExpected: true,
		},
		{
			name:          "no mandatory params but optional provided",
			params:        map[string]string{"port": "b"},
			errorExpected: true,
		},
		{
			name:          "partial mandatory params and optional provided",
			params:        map[string]string{"host": "a", "port": "b"},
			errorExpected: true,
		},
		{
			name:          "all params provided",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "port": "d", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "empty mandatory param",
			params:        map[string]string{"host": "", "user": "b", "dbName": "c"},
			errorExpected: true,
		},
		{
			name:          "empty port",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "mandatory params provided",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "password": "e", "streamingCfg": ""},
			errorExpected: true,
		},
		{
			name:          "mandatory params provided",
			params:        map[string]string{},
			errorExpected: false,
		},
	}

	setEnvVariavles()

	for _, tc := range testCases {
		s := SourceProfileDialectImpl{}
		g:= GetUtilInfoMock{}
		setGetInfoMockValues(&g)
		_, pgErr := s.NewSourceProfileConnectionPostgreSQL(tc.params, &g)
		_, mysqlErr := s.NewSourceProfileConnectionMySQL(tc.params, &g)
		assert.Equal(t, tc.errorExpected, pgErr != nil)
		assert.Equal(t, tc.errorExpected, mysqlErr != nil)
	}
}

func TestNewSourceProfileConnectionDynamoDB(t *testing.T) {
	// Avoid getting/settinng env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "no params",
			params:        map[string]string{},
			errorExpected: false,
		},
		{
			name:          "valid schema sample size",
			params:        map[string]string{"schema-sample-size": "15"},
			errorExpected: false,
		},
		{
			name:          "invalid schema sample size",
			params:        map[string]string{"schema-sample-size": "a"},
			errorExpected: true,
		},
	}

	for _, tc := range testCases {
		s := SourceProfileDialectImpl{}
		_, err := s.NewSourceProfileConnectionDynamoDB(tc.params)
		assert.Equal(t, tc.errorExpected, err != nil)
	}
}

func TestNewSourceProfileConnectionSqlServer(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "mandatory params provided",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "host not provided",
			params:        map[string]string{"user": "b", "dbName": "c", "password": "e"},
			errorExpected: true,
		},
		{
			name:          "user not provided",
			params:        map[string]string{"host": "a", "dbName": "c", "password": "e"},
			errorExpected: true,
		},
		{
			name:          "dbName not provided",
			params:        map[string]string{"host": "a", "user": "b", "password": "e"},
			errorExpected: true,
		},
		{
			name:          "all params provided",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "port": "d", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "empty host param",
			params:        map[string]string{"host": "", "user": "b", "dbName": "c"},
			errorExpected: true,
		},
		{
			name:          "empty user param",
			params:        map[string]string{"host": "a", "user": "", "dbName": "c"},
			errorExpected: true,
		},
		{
			name:          "empty dbName param",
			params:        map[string]string{"host": "a", "user": "b", "dbName": ""},
			errorExpected: true,
		},
		{
			name:          "empty password param",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "password": ""},
			errorExpected: false,
		},
		{
			name:          "empty port",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "port": "", "password": "e"},
			errorExpected: false,
		},
		{
			name:          "No param provided",
			params:        map[string]string{},
			errorExpected: true,
		},
	}

	for _, tc := range testCases {
		s := SourceProfileDialectImpl{}
		g:= GetUtilInfoMock{}
		setGetInfoMockValues(&g)
		_, sqlServer := s.NewSourceProfileConnectionSqlServer(tc.params, &g)
		assert.Equal(t, tc.errorExpected, sqlServer != nil)
	}
}

// code for testing oracle connection
func TestNewSourceProfileConnectionOracle(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "streamingCfg is blank",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "port": "d", "password": "e", "streamingCfg": ""},
			errorExpected: true,
		},
		{
			name:          "host is blank",
			params:        map[string]string{"host": "", "user": "b", "dbName": "c", "port": "d", "password": "e", "streamingCfg": "f"},
			errorExpected: true,
		},
		{
			name:          "user is blank",
			params:        map[string]string{"host": "a", "user": "", "dbName": "c", "port": "d", "password": "e", "streamingCfg": "f"},
			errorExpected: true,
		},
		{
			name:          "dbname is blank",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "", "port": "d", "password": "e", "streamingCfg": "f"},
			errorExpected: true,
		},
		{
			name:          "host is not specified",
			params:        map[string]string{"user": "b", "dbName": "c", "port": "d", "password": "e", "streamingCfg": "f"},
			errorExpected: true,
		},
		{
			name:          "user is not specified",
			params:        map[string]string{"host": "a", "dbName": "c", "port": "d", "password": "e", "streamingCfg": "f"},
			errorExpected: true,
		},
		{
			name:          "dbname is not specified",
			params:        map[string]string{"host": "a", "user": "b", "port": "d", "password": "e", "streamingCfg": "f"},
			errorExpected: true,
		},
		{
			name:          "port is blank",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "port": "", "password": "e", "streamingCfg": "f"},
			errorExpected: false,
		},
		{
			name:          "password is blank",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c", "port": "d", "password": "", "streamingCfg": "f"},
			errorExpected: false,
		},
	}

	for _, tc := range testCases {
		s := SourceProfileDialectImpl{}
		g:= GetUtilInfoMock{}
		setGetInfoMockValues(&g)
		_, oracleErr := s.NewSourceProfileConnectionOracle(tc.params, &g)
		assert.Equal(t, tc.errorExpected, oracleErr != nil)
	}
}


// code for testing cloud sql mysql connection
func TestNewSourceProfileConnectionCloudSQLMySQL(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "user is blank",
			params:        map[string]string{"dbName": "b", "instance": "c", "region": "d", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "dbname is blank",
			params:        map[string]string{"user": "a", "instance": "c", "region": "d", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "instance is blank",
			params:        map[string]string{"user": "a", "dbName": "b", "region": "d", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "region is blank",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "project is blank and util getProject () returns project successfully",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "region": "d",},
			errorExpected: false,
		},
		{
			name:          "project is blank and util getProject () fails",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "region": "d"},
			errorExpected: true,
		},
		{
			name:          "test runs successfully",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "region": "d", "project": "e"},
			errorExpected: false,
		},
	}

	for _, tc := range testCases {
		s := SourceProfileDialectImpl{}
		g:= GetUtilInfoMock{}
		setGetInfoMockValues(&g)
		if tc.name == "project is blank and util getProject () fails" {
			g.On("GetProject").Return("", fmt.Errorf("error"))
		} else {
			g.On("GetProject").Return("project-id", nil)
		}
		_, mysqlErr := s.NewSourceProfileConnectionCloudSQLMySQL(tc.params, &g)
		assert.Equal(t, tc.errorExpected, mysqlErr != nil)
	}
}


// code for testing postgres sql source connection profile
func TestNewSourceProfileConnectionCloudSQLPostgreSQL(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          string
		params        map[string]string
		errorExpected bool
	}{
		{
			name:          "user is blank",
			params:        map[string]string{"dbName": "b", "instance": "c", "region": "d", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "dbname is blank",
			params:        map[string]string{"user": "a", "instance": "c", "region": "d", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "instance is blank",
			params:        map[string]string{"user": "a", "dbName": "b", "region": "d", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "region is blank",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "project": "e"},
			errorExpected: true,
		},
		{
			name:          "project is blank and util getProject () returns project successfully",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "region": "d",},
			errorExpected: false,
		},
		{
			name:          "project is blank and util getProject () fails",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "region": "d"},
			errorExpected: true,
		},
		{
			name:          "test runs successfully",
			params:        map[string]string{"user": "a", "dbName": "b", "instance": "c", "region": "d", "project": "e"},
			errorExpected: false,
		},
	}

	for _, tc := range testCases {
		s := SourceProfileDialectImpl{}
		g:= GetUtilInfoMock{}
		setGetInfoMockValues(&g)
		if tc.name == "project is blank and util getProject () fails" {
			g.On("GetProject").Return("", fmt.Errorf("error"))
		} else {
			g.On("GetProject").Return("project-id", nil)
		}
		_, mysqlErr := s.NewSourceProfileConnectionCloudSQLPostgreSQL(tc.params, &g)
		assert.Equal(t, tc.errorExpected, mysqlErr != nil)
	}
}