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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"
)

type MockSourceProfileDialect struct {
    mock.Mock
}

func (m *MockSourceProfileDialect) NewSourceProfileConnectionCloudSQLMySQL(params map[string]string, g utils.GetUtilInfoInterface) (SourceProfileConnectionCloudSQLMySQL, error) {
    args := m.Called(params, g)
    return args.Get(0).(SourceProfileConnectionCloudSQLMySQL), args.Error(1)
}

func (m *MockSourceProfileDialect) NewSourceProfileConnectionMySQL(params map[string]string, g utils.GetUtilInfoInterface) (SourceProfileConnectionMySQL, error) {
    args := m.Called(params, g)
    return args.Get(0).(SourceProfileConnectionMySQL), args.Error(1)
}

func (m *MockSourceProfileDialect) NewSourceProfileConnectionCloudSQLPostgreSQL(params map[string]string, g utils.GetUtilInfoInterface) (SourceProfileConnectionCloudSQLPostgreSQL, error) {
    args := m.Called(params, g)
    return args.Get(0).(SourceProfileConnectionCloudSQLPostgreSQL), args.Error(1)
}

func (m *MockSourceProfileDialect) NewSourceProfileConnectionPostgreSQL(params map[string]string, g utils.GetUtilInfoInterface) (SourceProfileConnectionPostgreSQL, error) {
    args := m.Called(params, g)
    return args.Get(0).(SourceProfileConnectionPostgreSQL), args.Error(1)
}

func (m *MockSourceProfileDialect) NewSourceProfileConnectionSqlServer(params map[string]string, g utils.GetUtilInfoInterface) (SourceProfileConnectionSqlServer, error) {
    args := m.Called(params, g)
    return args.Get(0).(SourceProfileConnectionSqlServer), args.Error(1)
}

func (m *MockSourceProfileDialect) NewSourceProfileConnectionDynamoDB(params map[string]string, g utils.GetUtilInfoInterface) (SourceProfileConnectionDynamoDB, error) {
    args := m.Called(params, g)
    return args.Get(0).(SourceProfileConnectionDynamoDB), args.Error(1)
}

func (m *MockSourceProfileDialect) NewSourceProfileConnectionOracle(params map[string]string, g utils.GetUtilInfoInterface) (SourceProfileConnectionOracle, error) {
    args := m.Called(params, g)
    return args.Get(0).(SourceProfileConnectionOracle), args.Error(1)
}

func setEnvVariables(){
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

func unsetEnvVariables(){
	// My Sql Server Connection
	os.Setenv("MSSQL_IP_ADDRESS", "")
	os.Setenv("MSSQL_SA_USER", "")
	os.Setenv("MSSQL_DATABASE", "")
	os.Setenv("MSSQL_TCP_PORT", "")
	os.Setenv("MSSQL_SA_PASSWORD", "")
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

type MockNewSourceProfile struct {
    mock.Mock
}

func (nspm *MockNewSourceProfile) NewSourceProfileFile(params map[string]string) SourceProfileFile {
	args := nspm.Called()
	return args.Get(0).(SourceProfileFile)
}

func (nspm *MockNewSourceProfile) NewSourceProfileConfig(source string, path string) (SourceProfileConfig, error) {
	args := nspm.Called()
	return args.Get(0).(SourceProfileConfig), args.Error(1)
}

func (nspm *MockNewSourceProfile) NewSourceProfileConnectionCloudSQL(source string, params map[string]string, s SourceProfileDialectInterface) (SourceProfileConnectionCloudSQL, error) {
	args := nspm.Called()
	return args.Get(0).(SourceProfileConnectionCloudSQL), args.Error(1)
}

func (nspm *MockNewSourceProfile) NewSourceProfileConnection(source string, params map[string]string, s SourceProfileDialectInterface) (SourceProfileConnection, error) {
	args := nspm.Called()
	return args.Get(0).(SourceProfileConnection), args.Error(1)
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

		n := NewSourceProfileImpl{}
		profile := n.NewSourceProfileFile(tc.params)
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
		n := NewSourceProfileImpl{}
		sourceProfileConfig, err := n.NewSourceProfileConfig(tc.source, tc.path)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
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
		{
			name:          "empty password",
			params:        map[string]string{"host": "a", "user": "b", "dbName": "c"},
			errorExpected: false,
		},
	}

	before := func(){
		setEnvVariables()
	}

	after := func(){
		unsetEnvVariables()
	}

	for _, tc := range testCases {
		before()
		s := SourceProfileDialectImpl{}
		g:= GetUtilInfoMock{}
		setGetInfoMockValues(&g)
		_, pgErr := s.NewSourceProfileConnectionPostgreSQL(tc.params, &g)
		_, mysqlErr := s.NewSourceProfileConnectionMySQL(tc.params, &g)
		assert.Equal(t, tc.errorExpected, pgErr != nil, tc.name)
		assert.Equal(t, tc.errorExpected, mysqlErr != nil, tc.name)
		after()
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
		{
			name:          "valid aws access key id ",
			params:        map[string]string{"aws-access-key-id": "hdsjg"},
			errorExpected: false,
		},
		{
			name:          "valid aws region",
			params:        map[string]string{"aws-region": "us-central"},
			errorExpected: false,
		},
		{
			name:          "valid dydb endpoint",
			params:        map[string]string{"dydb-endpoint": "0.0.0.0"},
			errorExpected: false,
		},
		{
			name:          "enable streaming true",
			params:        map[string]string{"enableStreaming": "true"},
			errorExpected: false,
		},
		{
			name:          "enable streaming false",
			params:        map[string]string{"enableStreaming": "false"},
			errorExpected: false,
		},
		{
			name:          "invalid enable streaming",
			params:        map[string]string{"enableStreaming": "ujeh"},
			errorExpected: true,
		},
	}

	for _, tc := range testCases {
		s := SourceProfileDialectImpl{}
		_, err := s.NewSourceProfileConnectionDynamoDB(tc.params, &GetUtilInfoMock{})
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
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
			errorExpected: false,
		},
	}

	before := func(){
		setEnvVariables()
	}

	after := func(){
		unsetEnvVariables()
	}

	for _, tc := range testCases {
		before()
		s := SourceProfileDialectImpl{}
		g:= GetUtilInfoMock{}
		setGetInfoMockValues(&g)
		_, sqlServer := s.NewSourceProfileConnectionSqlServer(tc.params, &g)
		assert.Equal(t, tc.errorExpected, sqlServer != nil, tc.name)
		after()
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
		assert.Equal(t, tc.errorExpected, oracleErr != nil, tc.name)
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
		assert.Equal(t, tc.errorExpected, mysqlErr != nil, tc.name)
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
		assert.Equal(t, tc.errorExpected, mysqlErr != nil, tc.name)
	}
}

// code for testing new source connection profile
func TestNewSourceProfileConnection(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          string
		source		  string
		params        map[string]string
		function      string
		returnConnProfile interface{} 
		errorExpected bool
	}{
		{
			name:      		    "source mysql",
			source: 			"mysql",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionMySQL",
			returnConnProfile:  SourceProfileConnectionMySQL{},
			errorExpected: 		false,
		},
		{
			name:      		    "source postgresql",
			source: 			"postgresql",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionPostgreSQL",
			returnConnProfile:  SourceProfileConnectionPostgreSQL{},
			errorExpected: 		false,
		},
		{
			name:      		    "source dynamodb",
			source: 			"dynamodb",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionDynamoDB",
			returnConnProfile:  SourceProfileConnectionDynamoDB{},
			errorExpected: 		false,
		},
		{
			name:      		    "source sqlserver",
			source: 			"sqlserver",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionSqlServer",
			returnConnProfile:  SourceProfileConnectionSqlServer{},
			errorExpected: 		false,
		},
		{
			name:      		    "source oracle",
			source: 			"oracle",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionOracle",
			returnConnProfile:  SourceProfileConnectionOracle{},
			errorExpected: 		false,
		},
		{
			name:      		    "invalid source",
			source: 			"invalid",	
			params:    	    	map[string]string{},
			function:		   "",
			returnConnProfile:  nil,
			errorExpected: 		true,
		},
	}

	for _, tc := range testCases {
		m := MockSourceProfileDialect{}
		m.On(tc.function, mock.Anything, mock.Anything).Return(tc.returnConnProfile, nil)
		n := NewSourceProfileImpl{}
		_, err := n.NewSourceProfileConnection(tc.source, tc.params, &m)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		if err == nil {
			m.AssertExpectations(t) 
		}
	}
}

// code for testing cloud sql source connection profile
func TestNewSourceProfileConnectionCloudSQL(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          		string
		source		  		string
		params       	 	map[string]string
		function       		string
		returnConnProfile 	interface{} 
		returnError			error
		errorExpected		bool
	}{
		{
			name:      		    "source mysql",
			source: 			"mysql",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionCloudSQLMySQL",
			returnConnProfile:  SourceProfileConnectionCloudSQLMySQL{},
			returnError:		nil,
			errorExpected: 		false,
		},
		{
			name:      		    "source mysql",
			source: 			"mysql",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionCloudSQLMySQL",
			returnConnProfile:  SourceProfileConnectionCloudSQLMySQL{},
			returnError:		fmt.Errorf("error"),
			errorExpected: 		true,
		},
		{
			name:      		    "source postgresql error",
			source: 			"postgresql",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionCloudSQLPostgreSQL",
			returnConnProfile:  SourceProfileConnectionCloudSQLPostgreSQL{},
			returnError:		nil,
			errorExpected: 		false,
		},
		{
			name:      		    "source postgres error",
			source: 			"postgresql",	
			params:    	    	map[string]string{},
			function:		   "NewSourceProfileConnectionCloudSQLPostgreSQL",
			returnConnProfile:  SourceProfileConnectionCloudSQLPostgreSQL{},
			returnError:		fmt.Errorf("error"),
			errorExpected: 		true,
		},
	}

	for _, tc := range testCases {
		m := MockSourceProfileDialect{}
		m.On(tc.function, mock.Anything, mock.Anything).Return(tc.returnConnProfile, tc.returnError)
		n := NewSourceProfileImpl{}
		_, err := n.NewSourceProfileConnectionCloudSQL(tc.source, tc.params, &m)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		m.AssertExpectations(t)
	}
}

// code for testing csv source profile
func TestNewSourceProfileCsv(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          		string
		params       	 	map[string]string
		returnCsvProfile    SourceProfileCsv
	}{
		{
			name:      		    "default params",
			params:    	    	map[string]string{"manifest": "manifest.txt"},
			returnCsvProfile:   SourceProfileCsv{Manifest: "manifest.txt", Delimiter: ",", NullStr: ""},
		},
		{
			name:      		    "override delimiter",
			params:    	    	map[string]string{"manifest": "manifest.txt", "delimiter": "/"},
			returnCsvProfile:   SourceProfileCsv{Manifest: "manifest.txt", Delimiter: "/", NullStr: ""},
		},
		{
			name:      		    "override nulltr",
			params:    	    	map[string]string{"manifest": "manifest.txt", "nullStr": "/n"},
			returnCsvProfile:   SourceProfileCsv{Manifest: "manifest.txt", Delimiter: ",", NullStr: "/n"},
		},
	}

	for _, tc := range testCases {
		res := NewSourceProfileCsv(tc.params)
		assert.Equal(t, res, tc.returnCsvProfile, tc.name)
	}
}

// code to test boolean UseTargetSchema functionality
func TestUseTargetSchema(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          		string
		srcDriver       	string
		returnBoolean    	bool
	}{
		{
			name:      		    "csv as source driver",
			srcDriver: 			"csv",
			returnBoolean: 		true,		
		},
		{
			name:      		    "not csv as source driver",
			srcDriver: 			"cfg",
			returnBoolean: 		false,		
		},
	}
	for _, tc := range testCases {
		src := SourceProfile{
			Driver: tc.srcDriver,
		}
		res := src.UseTargetSchema()
		assert.Equal(t, res, tc.returnBoolean, tc.name)
	}
}


// code to test legacy driver
func TestToLegacyDriver(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	const (
		SourceProfileTypeUnset = iota
		SourceProfileTypeFile
		SourceProfileTypeConnection
		SourceProfileTypeConfig
		SourceProfileTypeCsv
		SourceProfileTypeCloudSQL
		InvalidType
	)
	testCases := []struct {
		name          		string
		srcDriver       	SourceProfile
		source  			string
		returnConstant    	string
		errorExpected		bool
	}{
		{
			name:      		    "source profile type FILE and source mysql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeFile},
			source: 			"mysql",
			returnConstant: 	constants.MYSQLDUMP,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type FILE and source postgresql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeFile},
			source: 			"postgresql",
			returnConstant: 	constants.PGDUMP,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type FILE and source dynamodb",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeFile},
			source: 			"dynamodb",
			returnConstant: 	"",	
			errorExpected:		true,
		},
		{
			name:      		    "source profile type FILE and source invalid",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeFile},
			source: 			"invalid",
			returnConstant: 	"",	
			errorExpected:		true,
		},
		{
			name:      		    "source profile type CONNECTION and source mysql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConnection},
			source: 			"mysql",
			returnConstant: 	constants.MYSQL,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CONNECTION and source postgresql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConnection},
			source: 			"postgresql",
			returnConstant: 	constants.POSTGRES,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CONNECTION and source dynamodb",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConnection},
			source: 			"dynamodb",
			returnConstant: 	constants.DYNAMODB,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CONNECTION and source mssql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConnection},
			source: 			"mssql",
			returnConstant: 	constants.SQLSERVER,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CONNECTION and source oracle",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConnection},
			source: 			"oracle",
			returnConstant: 	constants.ORACLE,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CONNECTION and source invalid",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConnection},
			source: 			"invalid",
			returnConstant: 	"",	
			errorExpected:		true,
		},
		{
			name:      		    "source profile type CLOUD SQL and source mysql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeCloudSQL},
			source: 			"mysql",
			returnConstant: 	constants.MYSQL,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CLOUD SQL and source postgresql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeCloudSQL},
			source: 			"postgresql",
			returnConstant: 	constants.POSTGRES,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CLOUD SQL and source invalid",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeCloudSQL},
			source: 			"invalid",
			returnConstant: 	"",	
			errorExpected:		true,
		},
		{
			name:      		    "source profile type CONFIG and source mysql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConfig},
			source: 			"mysql",
			returnConstant: 	constants.MYSQL,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CONFIG and source invalid",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeConfig},
			source: 			"invalid",
			returnConstant: 	"",	
			errorExpected:		true,
		},
		{
			name:      		    "source profile type CSV and source mysql",
			srcDriver: 			SourceProfile{Ty: SourceProfileTypeCsv},
			source: 			"",
			returnConstant: 	constants.CSV,	
			errorExpected:		false,
		},
		{
			name:      		    "source profile type CONFIG and source invalid",
			srcDriver: 			SourceProfile{Ty: InvalidType},
			source: 			"",
			returnConstant: 	"",	
			errorExpected:		true,
		},
	}
	for _, tc := range testCases {
		src := tc.srcDriver
		res, err := src.ToLegacyDriver(tc.source)
		assert.Equal(t, res, tc.returnConstant, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
	}
}



// code for testing new source profile
func TestNewSourceProfile(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	const (
		SourceProfileTypeUnset = iota
		SourceProfileTypeFile
		SourceProfileTypeConnection
		SourceProfileTypeConfig
		SourceProfileTypeCsv
		SourceProfileTypeCloudSQL
	)
	testCases := []struct {
		name          		string
		params		  		string
		source        		string
		function 			string
		mockReturn			interface{}
		returnTy			int
		errorExpected 		bool
	}{
		{
			name:          "source profile for file",
			params:        "file='file.txt'",
			source: 	   "file",
			function: 	   "NewSourceProfileFile",
			mockReturn:    SourceProfileFile{},
			returnTy:	   SourceProfileTypeFile,
			errorExpected: false,
		},
		{
			name:          "invalid source profile for file",
			params:        "format='some-format'",
			source: 	   "file",
			function: 	   "",
			mockReturn:    SourceProfileFile{},
			returnTy:	   SourceProfileTypeFile,
			errorExpected: true,
		},
		{
			name:          "source profile for config",
			params:        "config='file.cfg'",
			source: 	   "cfg",
			function: 	   "NewSourceProfileConfig",
			mockReturn:    SourceProfileConfig{},
			returnTy:	   SourceProfileTypeConfig,
			errorExpected: false,
		},
		{
			name:          "source profile for cloud sql instance",
			params:        "instance='instance'",
			source: 	   "instance",
			function: 	   "NewSourceProfileConnectionCloudSQL",
			mockReturn:    SourceProfileConnectionCloudSQL{},
			returnTy:	   SourceProfileTypeCloudSQL,
			errorExpected: false,
		},
		{
			name:          "source profile for csv",
			params:        "",
			source: 	   "csv",
			function: 	   "",
			mockReturn:    SourceProfile{},
			returnTy:	   SourceProfileTypeCsv,
			errorExpected: false,
		},
		{
			name:          "unset source profile params",
			params:        "",
			source: 	   "source",
			function: 	   "NewSourceProfileConnection",
			mockReturn:    SourceProfileConnection{},
			returnTy:	   SourceProfileTypeConnection,
			errorExpected: false,
		},
		{
			name:          "unset source",
			params:        "",
			source: 	   "",
			function: 	   "",
			mockReturn:    SourceProfile{},
			returnTy:	   SourceProfileTypeUnset,
			errorExpected: true,
		},
	}

	for _, tc := range testCases {
		n := MockNewSourceProfile{}
		n.On(tc.function, mock.Anything, mock.Anything, mock.Anything).Return(tc.mockReturn, nil)
		res, err := NewSourceProfile(tc.params, tc.source, &n)
		assert.Equal(t, SourceProfileType(tc.returnTy), res.Ty, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
	}
}