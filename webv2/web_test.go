package webv2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	ca "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/cassandra"
	cc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	helpers "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	neo4jauth "github.com/neo4j/neo4j-go-driver/v6/neo4j/auth"
	neo4jconfig "github.com/neo4j/neo4j-go-driver/v6/neo4j/config"
	"github.com/stretchr/testify/assert"
)


func TestValidateCassandraConnection(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Host: "127.0.0.1", Port: "9042", Database: "test_keyspace", DataCenter: "dc1", User: "user", Password: "pass",
	}

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		metadata, err := validateCassandraConnection(config)
		assert.NoError(t, err)
		assert.Equal(t, mockMetadata, metadata)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		metadata, err := validateCassandraConnection(config)
		assert.Nil(t, metadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Cassandra connection error")
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestDatabaseConnectionCassandra(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Driver:     constants.CASSANDRA,
		Host:       "127.0.0.1",
		Port:       "9042",
		Database:   "test_keyspace",
		DataCenter: "dc1",
		User:       "user",
		Password:   "pass",
	}
	configJSON, _ := json.Marshal(config)

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		sessionState := session.GetSessionState()
		assert.Equal(t, constants.CASSANDRA, sessionState.Driver)
		assert.Equal(t, "test_keyspace", sessionState.DbName)
		assert.Equal(t, mockMetadata, sessionState.KeyspaceMetadata)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Cassandra connection error")
	})
}

func TestSetSourceDBDetailsForDirectConnectCassandra(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Driver:     constants.CASSANDRA,
		Host:       "127.0.0.1",
		Port:       "9042",
		Database:   "test_keyspace",
		DataCenter: "dc1",
		User:       "user",
		Password:   "pass",
	}
	configJSON, _ := json.Marshal(config)

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		req, _ := http.NewRequest("POST", "/SetSourceDBDetailsForDirectConnect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setSourceDBDetailsForDirectConnect)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		sessionState := session.GetSessionState()
		assert.Equal(t, "test_keyspace", sessionState.DbName)
		assert.Equal(t, mockMetadata, sessionState.KeyspaceMetadata)
		assert.Equal(t, "127.0.0.1", sessionState.SourceDBConnDetails.Host)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		req, _ := http.NewRequest("POST", "/SetSourceDBDetailsForDirectConnect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setSourceDBDetailsForDirectConnect)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Cassandra connection error")
	})
}

func TestGetSourceAndTargetProfiles_Cassandra(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Conv = internal.MakeConv()
	sessionState.Driver = constants.CASSANDRA
	sessionState.DbName = "test_keyspace"
	sessionState.Dialect = constants.DIALECT_GOOGLESQL
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Host:           "127.0.0.1",
		Port:           "9042",
		User:           "user",
		Password:       "pass",
		DataCenter:     "dc1",
		ConnectionType: helpers.DIRECT_CONNECT_MODE,
	}
	sessionState.SpannerProjectId = "test-project"
	sessionState.SpannerInstanceID = "test-instance"

	details := types.MigrationDetails{
		TargetDetails: types.TargetDetails{
			TargetDB: "test-spanner-db",
		},
		MigrationMode: helpers.SCHEMA_ONLY,
	}

	sourceProfile, _, _, _, err := getSourceAndTargetProfiles(context.Background(), sessionState, details)

	assert.NoError(t, err)
	assert.Equal(t, constants.CASSANDRA, sourceProfile.Driver)
	assert.Equal(t, "127.0.0.1", sourceProfile.Conn.Cassandra.Host)
	assert.Equal(t, "9042", sourceProfile.Conn.Cassandra.Port)
	assert.Equal(t, "user", sourceProfile.Conn.Cassandra.User)
	assert.Equal(t, "pass", sourceProfile.Conn.Cassandra.Pwd)
	assert.Equal(t, "test_keyspace", sourceProfile.Conn.Cassandra.Keyspace)
	assert.Equal(t, "dc1", sourceProfile.Conn.Cassandra.DataCenter)
}

func TestGetSourceAndTargetProfiles_Neo4j(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Conv = internal.MakeConv()
	sessionState.Driver = constants.NEO4J
	sessionState.DbName = "neo4j"
	sessionState.Dialect = constants.DIALECT_GOOGLESQL
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Host:           "127.0.0.1",
		Port:           "7687",
		User:           "neo4j",
		Password:       "pass",
		ConnectionType: helpers.DIRECT_CONNECT_MODE,
	}
	sessionState.SpannerProjectId = "test-project"
	sessionState.SpannerInstanceID = "test-instance"

	details := types.MigrationDetails{
		TargetDetails: types.TargetDetails{
			TargetDB: "test-spanner-db",
		},
		MigrationMode: helpers.SCHEMA_ONLY,
	}

	sourceProfile, _, _, _, err := getSourceAndTargetProfiles(context.Background(), sessionState, details)

	assert.NoError(t, err)
	assert.Equal(t, constants.NEO4J, sourceProfile.Driver)
	assert.Equal(t, "bolt://127.0.0.1:7687", sourceProfile.Conn.Neo4j.URI)
	assert.Equal(t, "neo4j", sourceProfile.Conn.Neo4j.User)
	assert.Equal(t, "pass", sourceProfile.Conn.Neo4j.Pwd)
}

func TestCreateDatabaseConnectionString(t *testing.T) {
	testCases := []struct {
		name           string
		config         types.DriverConfig
		expectedString string
		expectError    bool
	}{
		{
			name: "Postgres driver",
			config: types.DriverConfig{
				Driver:   constants.POSTGRES,
				Host:     "localhost",
				Port:     "5432",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "postgres://user:passw%5C%60~ord@localhost:5432/testdb?sslmode=disable",
			expectError:    false,
		},
		{
			name: "MySQL driver",
			config: types.DriverConfig{
				Driver:   constants.MYSQL,
				Host:     "localhost",
				Port:     "3306",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "user:passw\\`~ord@tcp(localhost:3306)/testdb",
			expectError:    false,
		},
		{
			name: "SQL Server driver",
			config: types.DriverConfig{
				Driver:   constants.SQLSERVER,
				Host:     "localhost",
				Port:     "1433",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "sqlserver://user:passw%5C%60~ord@localhost:1433/testdb?sslmode=disable",
			expectError:    false,
		},
		{
			name: "Oracle driver",
			config: types.DriverConfig{
				Driver:   constants.ORACLE,
				Host:     "localhost",
				Port:     "1521",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "oracle://user:passw%5C%60~ord@localhost:1521/testdb",
			expectError:    false,
		},
		{
			name: "Unsupported driver",
			config: types.DriverConfig{
				Driver:   "unsupported",
				Host:     "localhost",
				Port:     "1234",
				User:     "user",
				Password: "password",
				Database: "testdb",
			},
			expectedString: "",
			expectError:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualString, err := createDatabaseConnectionString(tc.config)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedString, actualString)
			}
		})
	}
}

type mockNeo4jDriver struct {
	neo4j.Driver
	mockVerifyConnectivity func(ctx context.Context) error
	mockClose              func(ctx context.Context) error
}

func (m *mockNeo4jDriver) VerifyConnectivity(ctx context.Context) error {
	if m.mockVerifyConnectivity != nil {
		return m.mockVerifyConnectivity(ctx)
	}
	return nil
}

func (m *mockNeo4jDriver) Close(ctx context.Context) error {
	if m.mockClose != nil {
		return m.mockClose(ctx)
	}
	return nil
}

func TestDatabaseConnectionNeo4j(t *testing.T) {
	originalNewNeo4jDriver := newNeo4jDriver
	defer func() {
		newNeo4jDriver = originalNewNeo4jDriver
	}()

	config := types.DriverConfig{
		Driver:   constants.NEO4J,
		Host:     "127.0.0.1",
		Port:     "7687",
		User:     "neo4j",
		Password: "password",
	}
	configJSON, _ := json.Marshal(config)

	t.Run("Success", func(t *testing.T) {
		mockDriver := &mockNeo4jDriver{}
		newNeo4jDriver = func(target string, token neo4jauth.TokenManager, configurers ...func(*neo4jconfig.Config)) (neo4j.Driver, error) {
			assert.Equal(t, "bolt://127.0.0.1:7687", target)
			return mockDriver, nil
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		sessionState := session.GetSessionState()
		assert.Equal(t, constants.NEO4J, sessionState.Driver)
		assert.Nil(t, sessionState.SourceDB)
	})

	t.Run("Driver Creation Failure", func(t *testing.T) {
		expectedErr := errors.New("driver creation failed")
		newNeo4jDriver = func(target string, token neo4jauth.TokenManager, configurers ...func(*neo4jconfig.Config)) (neo4j.Driver, error) {
			return nil, expectedErr
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Neo4j driver creation error")
	})

	t.Run("Connectivity Verification Failure", func(t *testing.T) {
		expectedErr := errors.New("verification failed")
		mockDriver := &mockNeo4jDriver{
			mockVerifyConnectivity: func(ctx context.Context) error {
				return expectedErr
			},
		}
		newNeo4jDriver = func(target string, token neo4jauth.TokenManager, configurers ...func(*neo4jconfig.Config)) (neo4j.Driver, error) {
			return mockDriver, nil
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Neo4j connection error")
	})
}
