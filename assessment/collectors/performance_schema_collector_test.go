// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package assessment

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sourcesCommon "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql" // Need to import this for type assertion
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop() // Disable logging for tests
}

// MockPerformanceSchema mocks sourcesCommon.PerformanceSchema
type MockPerformanceSchema struct {
	mock.Mock
}

func (m *MockPerformanceSchema) GetAllQueryAssessments() ([]utils.QueryAssessmentInfo, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]utils.QueryAssessmentInfo), args.Error(1)
}

func Test_getPerformanceSchema(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Test Case 1: MySQL Driver (Supported)
	sourceProfileMySQL := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_mysql_db",
			},
		},
	}
	psMySQL, err := getPerformanceSchema(db, sourceProfileMySQL)
	assert.NoError(t, err)
	assert.NotNil(t, psMySQL)

	mysqlPS, ok := psMySQL.(mysql.PerformanceSchemaImpl)
	assert.True(t, ok, "Expected mysql.PerformanceSchemaImpl type")
	assert.Equal(t, db, mysqlPS.Db)
	assert.Equal(t, "test_mysql_db", mysqlPS.DbName)

	// Test Case 2: Unsupported Driver
	sourceProfileUnsupported := profiles.SourceProfile{
		Driver: "unsupported_db",
	}
	psUnsupported, err := getPerformanceSchema(db, sourceProfileUnsupported)
	assert.Error(t, err)
	assert.Nil(t, psUnsupported)
	assert.Contains(t, err.Error(), "driver unsupported_db not supported for performance schema")
}

func TestPerformanceSchemaCollector_IsEmpty(t *testing.T) {
	tests := []struct {
		name      string
		collector PerformanceSchemaCollector
		want      bool
	}{
		{
			name:      "empty collector",
			collector: PerformanceSchemaCollector{},
			want:      true,
		},
		{
			name: "collector with queries",
			collector: PerformanceSchemaCollector{
				queries: []utils.QueryAssessmentInfo{{Query: "SELECT * FROM users"}},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.collector.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetPerformanceSchemaCollector_Success(t *testing.T) {
	dummyDb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating dummy sqlmock DB: %v", err)
	}
	defer dummyDb.Close()

	sourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_db",
			},
		},
	}
	expectedQueries := []utils.QueryAssessmentInfo{
		{Query: "SELECT * FROM users", Db: utils.DbIdentifier{DatabaseName: "test_db"}, Count: 100},
		{Query: "INSERT INTO products", Db: utils.DbIdentifier{DatabaseName: "test_db"}, Count: 50},
	}

	mockCfgProvider := new(MockConnectionConfigProvider)
	mockDbConnector := new(MockDBConnector)
	mockPS := new(MockPerformanceSchema)

	mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("mock_conn_string", nil).Once()
	mockDbConnector.On("Connect", sourceProfile.Driver, "mock_conn_string").Return(dummyDb, nil).Once()
	mockPS.On("GetAllQueryAssessments").Return(expectedQueries, nil).Once()

	mockPerformanceSchemaProvider := func(db *sql.DB, sp profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error) {
		assert.Equal(t, dummyDb, db) // Verify that the correct DB is passed.
		assert.Equal(t, sourceProfile, sp)
		return mockPS, nil
	}

	collector, err := GetPerformanceSchemaCollector(sourceProfile, mockDbConnector, mockCfgProvider, mockPerformanceSchemaProvider)

	assert.NoError(t, err)
	assert.NotNil(t, collector)
	assert.False(t, collector.IsEmpty())
	assert.Equal(t, expectedQueries, collector.queries)

	mockCfgProvider.AssertExpectations(t)
	mockDbConnector.AssertExpectations(t)
	mockPS.AssertExpectations(t)
}

func TestGetPerformanceSchemaCollector_ErrorFromGetConnectionConfig(t *testing.T) {
	sourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_db",
			},
		},
	}

	mockCfgProvider := new(MockConnectionConfigProvider)
	mockDbConnector := new(MockDBConnector)

	expectedErr := errors.New("failed to get connection config: mock config error")
	mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("", errors.New("mock config error")).Once()

	collector, err := GetPerformanceSchemaCollector(sourceProfile, mockDbConnector, mockCfgProvider, nil)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	assert.True(t, collector.IsEmpty())

	mockCfgProvider.AssertExpectations(t)
	mockDbConnector.AssertNotCalled(t, "Connect", mock.Anything, mock.Anything)
}

func TestGetPerformanceSchemaCollector_ErrorFromDBConnect(t *testing.T) {
	sourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_db",
			},
		},
	}

	mockCfgProvider := new(MockConnectionConfigProvider)
	mockDbConnector := new(MockDBConnector)

	expectedErr := errors.New("failed to connect to database: mock db connect error")
	mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("mock_conn_string", nil).Once()
	mockDbConnector.On("Connect", sourceProfile.Driver, "mock_conn_string").Return(nil, errors.New("mock db connect error")).Once()

	collector, err := GetPerformanceSchemaCollector(sourceProfile, mockDbConnector, mockCfgProvider, nil)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	assert.True(t, collector.IsEmpty())

	mockCfgProvider.AssertExpectations(t)
	mockDbConnector.AssertExpectations(t)
}

func TestGetPerformanceSchemaCollector_ErrorFromPerformanceSchemaProvider(t *testing.T) {
	dummyDb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating dummy sqlmock DB: %v", err)
	}
	defer dummyDb.Close()

	sourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_db",
			},
		},
	}

	mockCfgProvider := new(MockConnectionConfigProvider)
	mockDbConnector := new(MockDBConnector)

	expectedErr := errors.New("failed to get performance schema: mock provider error")
	mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("mock_conn_string", nil).Once()
	mockDbConnector.On("Connect", sourceProfile.Driver, "mock_conn_string").Return(dummyDb, nil).Once()

	mockPerformanceSchemaProvider := func(db *sql.DB, sp profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error) {
		return nil, errors.New("mock provider error")
	}

	collector, err := GetPerformanceSchemaCollector(sourceProfile, mockDbConnector, mockCfgProvider, mockPerformanceSchemaProvider)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	assert.True(t, collector.IsEmpty())

	mockCfgProvider.AssertExpectations(t)
	mockDbConnector.AssertExpectations(t)
}

func TestGetPerformanceSchemaCollector_ErrorFromGetAllQueryAssessments(t *testing.T) {
	dummyDb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating dummy sqlmock DB: %v", err)
	}
	defer dummyDb.Close()

	sourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_db",
			},
		},
	}

	mockCfgProvider := new(MockConnectionConfigProvider)
	mockDbConnector := new(MockDBConnector)
	mockPS := new(MockPerformanceSchema)

	expectedErr := errors.New("failed to get all queries: mock get all queries error")
	mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("mock_conn_string", nil).Once()
	mockDbConnector.On("Connect", sourceProfile.Driver, "mock_conn_string").Return(dummyDb, nil).Once()
	mockPS.On("GetAllQueryAssessments").Return(nil, errors.New("mock get all queries error")).Once()

	mockPerformanceSchemaProvider := func(db *sql.DB, sp profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error) {
		return mockPS, nil
	}

	collector, err := GetPerformanceSchemaCollector(sourceProfile, mockDbConnector, mockCfgProvider, mockPerformanceSchemaProvider)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedErr.Error())
	assert.True(t, collector.IsEmpty())

	mockCfgProvider.AssertExpectations(t)
	mockDbConnector.AssertExpectations(t)
	mockPS.AssertExpectations(t)
}
